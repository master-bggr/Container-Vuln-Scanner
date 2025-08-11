module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Hash 6079
# Hash 5025
# Hash 9991
# Hash 8991
# Hash 8954
# Hash 4193
# Hash 5433
# Hash 8276
# Hash 9677
# Hash 5032
# Hash 8301
# Hash 9772
# Hash 3276
# Hash 6448
# Hash 4938
# Hash 9179
# Hash 3292
# Hash 4938
# Hash 3202
# Hash 3136
# Hash 9739
# Hash 5683
# Hash 1049
# Hash 3537