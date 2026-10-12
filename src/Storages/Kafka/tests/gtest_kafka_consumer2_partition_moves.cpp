#include <config.h>
#include <gtest/gtest.h>

#if USE_RDKAFKA

#include <Storages/Kafka/KafkaConsumer2.h>
#include <Common/Logger.h>

#include <rdkafka_mock.h>
#include <cppkafka/cppkafka.h>
#include <librdkafka/rdkafka.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <string>

using namespace DB;
using namespace std::chrono_literals;

namespace
{

constexpr auto TOPIC = "kafka_consumer2_partition_moves";
constexpr int32_t PARTITION_COUNT = 3;
constexpr int32_t MESSAGES_PER_PARTITION = 3000;
constexpr size_t MESSAGE_SIZE = 1000;
constexpr auto WAIT_TIMEOUT = 30s;

/// Owns librdkafka's in-process mock cluster (it lives as long as the producer that created it)
/// and fills every partition of the topic.
struct MockKafka
{
    std::unique_ptr<cppkafka::Producer> producer;
    std::string bootstrap_servers;

    MockKafka()
    {
        producer = std::make_unique<cppkafka::Producer>(cppkafka::Configuration{
            {"test.mock.num.brokers", "1"},
            {"linger.ms", "5"},
            {"queue.buffering.max.kbytes", "1048576"},
        });
        auto * cluster = rd_kafka_handle_mock_cluster(producer->get_handle());
        if (rd_kafka_mock_topic_create(cluster, TOPIC, PARTITION_COUNT, 1) != RD_KAFKA_RESP_ERR_NO_ERROR)
            throw std::runtime_error("Cannot create the mock topic");
        bootstrap_servers = rd_kafka_mock_cluster_bootstraps(cluster);

        const std::string payload(MESSAGE_SIZE, 'x');
        for (int32_t partition = 0; partition < PARTITION_COUNT; ++partition)
            for (int32_t i = 0; i < MESSAGES_PER_PARTITION; ++i)
                producer->produce(cppkafka::MessageBuilder(TOPIC).partition(partition).payload(payload));
        producer->flush(60s);
    }
};

class KafkaConsumer2PartitionMoves : public ::testing::Test
{
protected:
    MockKafka kafka;
    std::atomic<bool> stopped{false};
    KafkaConsumer2Ptr consumer;

    void SetUp() override
    {
        consumer = std::make_shared<KafkaConsumer2>(
            getLogger("KafkaConsumer2PartitionMoves"),
            /* max_batch_size = */ 100,
            /* poll_timeout_ = */ 100,
            stopped,
            Names{TOPIC});
        consumer->createConsumer(
            cppkafka::Configuration{
                {"bootstrap.servers", kafka.bootstrap_servers},
                {"group.id", "kafka_consumer2_partition_moves"},
                {"enable.auto.commit", "false"},
                /// Small prefetch limit, so the partition queues fill in a moment (the default is 64 MiB per partition).
                {"queued.max.messages.kbytes", "1024"},
                {"queued.min.messages", "100000"},
                {"fetch.message.max.bytes", "65536"},
                {"fetch.queue.backoff.ms", "10"},
                /// The statistics are the only way to see the length of the partition queues from outside.
                {"statistics.interval.ms", "100"},
            });
    }

    void assign(std::initializer_list<int32_t> partitions)
    {
        KafkaConsumer2::TopicPartitionOffsets offsets;
        for (auto partition : partitions)
            offsets.push_back({{TOPIC, partition}, 0});
        consumer->updateOffsets(std::move(offsets));
    }

    /// Number of entries in librdkafka's fetch queue of the partition, from the latest statistics.
    Int64 queuedEntries(int32_t partition) const
    {
        const auto stat = consumer->getStat().rdkafka_stat;
        if (stat.empty())
            return -1;
        Poco::JSON::Parser parser;
        auto object = parser.parse(stat).extract<Poco::JSON::Object::Ptr>();
        for (const auto & key : {std::string("topics"), std::string(TOPIC), std::string("partitions"), std::to_string(partition)})
        {
            object = object->getObject(key);
            if (!object)
                return -1;
        }
        return object->getValue<Int64>("fetchq_cnt");
    }

    /// Serves the main consumer queue, which delivers the statistics, until the condition holds.
    bool waitUntil(const std::function<bool()> & condition) const
    {
        const auto deadline = std::chrono::steady_clock::now() + WAIT_TIMEOUT;
        while (std::chrono::steady_clock::now() < deadline)
        {
            consumer->pollEvents();
            if (condition())
                return true;
        }
        return false;
    }

    /// Offset of the first message read from the partition. `consume` returns nothing for another partition until the
    /// next assignment, so call it once per assignment.
    std::optional<int64_t> firstOffset(int32_t partition) const
    {
        const auto deadline = std::chrono::steady_clock::now() + WAIT_TIMEOUT;
        while (std::chrono::steady_clock::now() < deadline)
            if (consumer->consume({TOPIC, partition}, std::nullopt))
                return consumer->currentOffset();
        return std::nullopt;
    }
};

}

/// When a partition leaves the assignment, the messages librdkafka has already prefetched for it must be freed.
/// Before, they stayed in librdkafka's fetch queue of the partition, and nothing read or freed them.
TEST_F(KafkaConsumer2PartitionMoves, RemovedPartitionQueueIsEmptied)
{
    assign({0, 1, 2});
    ASSERT_TRUE(waitUntil([&] { return queuedEntries(1) > 100; })) << "partition 1 was not prefetched, queued " << queuedEntries(1);

    assign({0, 2});
    /// Only a couple of internal version barriers may stay.
    EXPECT_TRUE(waitUntil([&] { return queuedEntries(1) >= 0 && queuedEntries(1) < 10; }))
        << "prefetched messages of removed partition 1 were not freed, queued " << queuedEntries(1);
}

/// The removed partition is paused. It has to be fetched again when it comes back, and the partitions that stay
/// must not be affected.
TEST_F(KafkaConsumer2PartitionMoves, PartitionIsFetchedAgainWhenItComesBack)
{
    assign({0, 1, 2});
    ASSERT_TRUE(waitUntil([&] { return queuedEntries(1) > 100; }));

    assign({0, 2});
    EXPECT_EQ(firstOffset(0), 0);

    assign({0, 1, 2});
    EXPECT_EQ(firstOffset(1), 0);
}

#endif
