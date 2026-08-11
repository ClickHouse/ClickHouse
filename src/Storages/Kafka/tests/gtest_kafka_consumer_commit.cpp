#include <gtest/gtest.h>
#include <config.h>

#if USE_RDKAFKA

#include <Common/Stopwatch.h>
#include <cppkafka/configuration.h>
#include <cppkafka/consumer.h>
#include <cppkafka/exceptions.h>
#include <chrono>

using namespace std::chrono_literals;

namespace
{

cppkafka::Consumer makeConsumerAgainstUnreachableBroker()
{
    cppkafka::Configuration config;
    config.set("metadata.broker.list", "127.0.0.1:1");
    config.set("group.id", "gtest_kafka_consumer_commit");
    config.set("enable.auto.commit", "false");
    config.set("enable.auto.offset.store", "false");
    config.set("socket.timeout.ms", "1000");
    return cppkafka::Consumer(config);
}

cppkafka::TopicPartitionList oneOffset()
{
    return {cppkafka::TopicPartition{"gtest_kafka_commit_topic", 0, 42}};
}

template <typename Commit>
void expectTheDeadlineReturns(const Commit & commit)
{
    Stopwatch watch;
    try
    {
        commit();
        FAIL() << "the commit returned instead of reaching the deadline";
    }
    catch (const cppkafka::HandleException & e)
    {
        /// Any other code means something answered before the deadline, and an elapsed-time bound
        /// alone would then hold without the deadline being what returned.
        EXPECT_EQ(e.get_error().get_error(), RD_KAFKA_RESP_ERR__TIMED_OUT);
    }
    EXPECT_LT(watch.elapsedMilliseconds(), 15000);
}

}

/// Pins the deadline the Kafka consumers commit under. `cppkafka::Consumer::commit()` waits on
/// librdkafka's reply queue with no timeout, so against brokers that answer and then reject
/// authentication no reply op ever arrives, that call never returns, and the streaming task holding
/// the consumer stops its table until the server restarts. A refused connection is the stand-in a
/// unit test can build: librdkafka defers the commit and gives up `session.timeout.ms` later, 45s by
/// default, so without the deadline the elapsed-time bound is what fails. cppkafka's own suite needs
/// a live broker, so the timeout overload is covered here instead.
TEST(KafkaCommitWithTimeout, CurrentAssignmentRespectsTheDeadline)
{
    auto consumer = makeConsumerAgainstUnreachableBroker();
    /// A commit carrying no offset is answered `_NO_OFFSET` in 0ms, before the reply queue is ever
    /// polled, so a consumer holding no stored offset cannot reach the deadline.
    const cppkafka::TopicPartitionList stored = oneOffset();
    consumer.assign(stored);
    consumer.store_offsets(stored);

    expectTheDeadlineReturns([&] { consumer.commit(3000ms); });
}

/// Same, for the explicit-offsets form the Keeper-backed consumer uses, which also exercises the
/// `cppkafka::TopicPartitionList` conversion. Offsets passed in are committable on their own, so this
/// one needs no stored offset.
TEST(KafkaCommitWithTimeout, ExplicitOffsetsRespectTheDeadline)
{
    auto consumer = makeConsumerAgainstUnreachableBroker();
    const cppkafka::TopicPartitionList offsets = oneOffset();

    expectTheDeadlineReturns([&] { consumer.commit(offsets, 3000ms); });
}

#endif
