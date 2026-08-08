#include <gtest/gtest.h>
#include <config.h>

#if USE_RDKAFKA

#include <Storages/Kafka/KafkaConsumer.h>
#include <Common/Stopwatch.h>
#include <cppkafka/configuration.h>
#include <cppkafka/consumer.h>
#include <cppkafka/exceptions.h>
#include <chrono>

using namespace DB;
using namespace std::chrono_literals;

/// Pins the deadline itself. cppkafka's `commit` waits on librdkafka's reply queue with no timeout, and
/// against unreachable brokers no reply op ever arrives, so the call never returns and the streaming
/// task holding the consumer stops the table until the server restarts.
TEST(KafkaConsumerCommit, RespectsItsDeadlineAgainstUnreachableBrokers)
{
    cppkafka::Configuration config;
    config.set("metadata.broker.list", "127.0.0.1:1");
    config.set("group.id", "gtest_kafka_consumer_commit");
    config.set("enable.auto.commit", "false");
    config.set("socket.timeout.ms", "1000");

    cppkafka::Consumer consumer(config);

    /// A `DB::Exception` escaping here fails the test, which is what pins the event wiring: a wrong
    /// event type check reports `LOGICAL_ERROR` rather than a Kafka error.
    Stopwatch watch;
    try
    {
        commitCurrentAssignmentWithTimeout(consumer, 3000ms);
    }
    catch (const cppkafka::HandleException &)
    {
        /// Expected: with no assignment librdkafka answers locally, and an unreachable broker cannot
        /// answer at all. Either way the outcome is an error, not a hang.
    }

    EXPECT_LT(watch.elapsedMilliseconds(), 15000);
}

#endif
