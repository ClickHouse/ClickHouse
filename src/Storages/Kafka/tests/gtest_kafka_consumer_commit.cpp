#include <gtest/gtest.h>
#include <config.h>

#if USE_RDKAFKA

#include <Storages/Kafka/StorageKafkaUtils.h>
#include <Common/Stopwatch.h>
#include <cppkafka/configuration.h>
#include <cppkafka/consumer.h>
#include <cppkafka/exceptions.h>
#include <chrono>

using namespace DB;
using namespace std::chrono_literals;

namespace
{

cppkafka::Consumer makeConsumerAgainstUnreachableBroker()
{
    cppkafka::Configuration config;
    config.set("metadata.broker.list", "127.0.0.1:1");
    config.set("group.id", "gtest_kafka_consumer_commit");
    config.set("enable.auto.commit", "false");
    config.set("socket.timeout.ms", "1000");
    return cppkafka::Consumer(config);
}

}

/// Pins the deadline. cppkafka's own `commit` waits on librdkafka's reply queue with no timeout, and
/// against unreachable brokers no reply op arrives, so the call never returns and the streaming task
/// holding the consumer stops its table until the server restarts. A `DB::Exception` escaping here
/// fails the test too, which is what pins the event wiring. Without the deadline these tests hang
/// rather than fail - the call under test is the one that never returns.
TEST(KafkaCommitWithTimeout, CurrentAssignmentRespectsTheDeadline)
{
    auto consumer = makeConsumerAgainstUnreachableBroker();

    Stopwatch watch;
    EXPECT_THROW(StorageKafkaUtils::commitWithTimeout(consumer, nullptr, 3000ms), cppkafka::HandleException);
    EXPECT_LT(watch.elapsedMilliseconds(), 15000);
}

/// Same, for the explicit-offsets form the Keeper-backed consumer uses, which also exercises the
/// `cppkafka::TopicPartitionList` conversion.
TEST(KafkaCommitWithTimeout, ExplicitOffsetsRespectTheDeadline)
{
    auto consumer = makeConsumerAgainstUnreachableBroker();
    const cppkafka::TopicPartitionList offsets{cppkafka::TopicPartition{"gtest_kafka_commit_topic", 0, 42}};

    Stopwatch watch;
    EXPECT_THROW(StorageKafkaUtils::commitWithTimeout(consumer, &offsets, 3000ms), cppkafka::HandleException);
    EXPECT_LT(watch.elapsedMilliseconds(), 15000);
}

#endif
