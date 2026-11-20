#include <gtest/gtest.h>
#include <config.h>

#if USE_AMQPCPP

#include <Storages/RabbitMQ/RabbitMQProducer.h>
#include <Common/logger_useful.h>

#include <atomic>
#include <vector>

namespace DB
{

/// Drives the publisher confirms handler `RabbitMQProducer::removeRecord` directly. The delivery tag
/// of an ack or nack comes from the broker, so it is not guaranteed to be in `delivery_record`.
/// The producer is never initialized, so no connection to a broker is made.
class RabbitMQProducerTest : public ::testing::Test
{
protected:
    RabbitMQProducerTest()
        : publisher(RabbitMQConfiguration{}, Names{"key"}, "exchange", AMQP::ExchangeType::direct, 0, false, shutdown_called, getLogger("RabbitMQProducerTest"))
    {
    }

    void addRecords(std::initializer_list<UInt64> tags)
    {
        for (UInt64 tag : tags)
            publisher.delivery_record.emplace(tag, RabbitMQProducer::Payload{"message_" + std::to_string(tag), tag});
    }

    void removeRecord(UInt64 tag, bool multiple, bool republish) { publisher.removeRecord(tag, multiple, republish); }

    std::vector<UInt64> pendingTags() const
    {
        std::vector<UInt64> result;
        for (const auto & [tag, _] : publisher.delivery_record)
            result.push_back(tag);
        return result;
    }

    std::vector<UInt64> returnedIds()
    {
        std::vector<UInt64> result;
        RabbitMQProducer::Payload payload;
        while (publisher.returned.tryPop(payload))
            result.push_back(payload.id);
        return result;
    }

    std::atomic<bool> shutdown_called{false};
    RabbitMQProducer publisher;
};

TEST_F(RabbitMQProducerTest, SingleAckRemovesOnlyItsRecord)
{
    addRecords({1, 2, 3});
    removeRecord(2, /* multiple = */ false, /* republish = */ false);
    EXPECT_EQ(pendingTags(), (std::vector<UInt64>{1, 3}));
    EXPECT_TRUE(returnedIds().empty());
}

TEST_F(RabbitMQProducerTest, UnknownSingleAckOrNackIsIgnored)
{
    addRecords({1, 3});
    removeRecord(2, /* multiple = */ false, /* republish = */ false);
    removeRecord(2, /* multiple = */ false, /* republish = */ true);
    removeRecord(10, /* multiple = */ false, /* republish = */ true);
    EXPECT_EQ(pendingTags(), (std::vector<UInt64>{1, 3}));
    EXPECT_TRUE(returnedIds().empty());
}

TEST_F(RabbitMQProducerTest, RepeatedSingleNackRequeuesOnce)
{
    addRecords({1, 2});
    removeRecord(1, /* multiple = */ false, /* republish = */ true);
    removeRecord(1, /* multiple = */ false, /* republish = */ true);
    EXPECT_EQ(pendingTags(), (std::vector<UInt64>{2}));
    EXPECT_EQ(returnedIds(), (std::vector<UInt64>{1}));
}

TEST_F(RabbitMQProducerTest, MultipleAckWithMissingBoundaryTag)
{
    addRecords({1, 2, 4, 5});
    removeRecord(3, /* multiple = */ true, /* republish = */ false);
    EXPECT_EQ(pendingTags(), (std::vector<UInt64>{4, 5}));
    EXPECT_TRUE(returnedIds().empty());
}

TEST_F(RabbitMQProducerTest, MultipleNackWithMissingBoundaryTagRequeuesOnce)
{
    addRecords({1, 2, 4, 5});
    removeRecord(3, /* multiple = */ true, /* republish = */ true);
    removeRecord(3, /* multiple = */ true, /* republish = */ true);
    EXPECT_EQ(pendingTags(), (std::vector<UInt64>{4, 5}));
    EXPECT_EQ(returnedIds(), (std::vector<UInt64>{1, 2}));
}

TEST_F(RabbitMQProducerTest, MultipleNackBeyondAllRecords)
{
    addRecords({1, 2});
    removeRecord(100, /* multiple = */ true, /* republish = */ true);
    EXPECT_TRUE(pendingTags().empty());
    EXPECT_EQ(returnedIds(), (std::vector<UInt64>{1, 2}));
}

TEST_F(RabbitMQProducerTest, ConfirmsOnEmptyRecord)
{
    removeRecord(1, /* multiple = */ true, /* republish = */ false);
    removeRecord(1, /* multiple = */ true, /* republish = */ true);
    removeRecord(1, /* multiple = */ false, /* republish = */ false);
    removeRecord(1, /* multiple = */ false, /* republish = */ true);
    EXPECT_TRUE(pendingTags().empty());
    EXPECT_TRUE(returnedIds().empty());
}

}

#endif
