#pragma once

#include <chrono>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <base/types.h>
#include <cppkafka/configuration.h>
#include <cppkafka/cppkafka.h>
#include <cppkafka/topic_partition.h>
#include <fmt/ostream.h>
#include <Core/SettingsEnums.h>
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
Names parseTopics(String topic_list);
String getDefaultClientId(const StorageID & table_id);

using ErrorHandler = std::function<void(const cppkafka::Error &)>;

void drainConsumer(
    cppkafka::Consumer & consumer,
    std::chrono::milliseconds drain_timeout,
    const LoggerPtr & log,
    ErrorHandler error_handler = [](const cppkafka::Error & /*err*/) {});

using Messages = std::vector<cppkafka::Message>;
void eraseMessageErrors(Messages & messages, const LoggerPtr & log, ErrorHandler error_handler = [](const cppkafka::Error & /*err*/) {});

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
