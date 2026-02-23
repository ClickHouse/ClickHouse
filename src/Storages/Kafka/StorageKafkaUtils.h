#pragma once

#include <chrono>
#include <Common/Logger_fwd.h>
#include <Core/Names.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <base/types.h>
#include <boost/circular_buffer.hpp>
#include <cppkafka/configuration.h>
#include <cppkafka/cppkafka.h>
#include <cppkafka/topic_partition.h>
#include <fmt/ostream.h>
#include <librdkafka/rdkafka.h>
#include <Common/SettingsChanges.h>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{

class VirtualColumnsDescription;
struct KafkaSettings;

namespace StorageKafkaUtils
{

struct ConsumerStatistics // system.kafka_consumers data
{
    struct ExceptionInfo
    {
        String text;
        UInt64 timestamp;
    };
    using ExceptionsBuffer = boost::circular_buffer<ExceptionInfo>;
    struct Assignment
    {
        String topic_str;
        Int32 partition_id;
        Int64 current_offset;
        std::optional<Int64> intent_size;
    };
    using Assignments = std::vector<Assignment>;

    String consumer_id;
    Assignments assignments;
    UInt64 last_poll_time;
    UInt64 num_messages_read;
    UInt64 last_commit_timestamp;
    UInt64 last_rebalance_timestamp;
    UInt64 num_commits;
    UInt64 num_rebalance_assignments;
    UInt64 num_rebalance_revocations;
    ExceptionsBuffer exceptions_buffer;
    bool in_use;
    UInt64 last_used_usec;
    std::string rdkafka_stat;
};

Names parseTopics(String topic_list);
String getDefaultClientId(const StorageID & table_id);

using ErrorHandler = std::function<void(const cppkafka::Error &)>;

void consumerGracefulStop(
    cppkafka::Consumer & consumer,
    std::chrono::milliseconds drain_timeout,
    const LoggerPtr & log,
    ErrorHandler error_handler = [](const cppkafka::Error & /*err*/) {});

void consumerStopWithoutRebalance(
    cppkafka::Consumer & consumer,
    std::chrono::milliseconds drain_timeout,
    const LoggerPtr & log,
    ErrorHandler error_handler = [](const cppkafka::Error & /*err*/) {});

void drainConsumer(
    cppkafka::Consumer & consumer,
    std::chrono::milliseconds drain_timeout,
    const LoggerPtr & log,
    ErrorHandler error_handler = [](const cppkafka::Error & /*err*/) {});

using Messages = std::vector<cppkafka::Message>;
size_t eraseMessageErrors(Messages & messages, const LoggerPtr & log, ErrorHandler error_handler = [](const cppkafka::Error & /*err*/) {});

SettingsChanges createSettingsAdjustments(KafkaSettings & kafka_settings, const String & schema_name);

bool checkDependencies(const StorageID & table_id, const ContextPtr& context);

VirtualColumnsDescription createVirtuals(StreamingHandleErrorMode handle_error_mode);
}
}

template <>
struct fmt::formatter<cppkafka::TopicPartition> : fmt::ostream_formatter
{
};
template <>
struct fmt::formatter<cppkafka::Error> : fmt::ostream_formatter
{
};
