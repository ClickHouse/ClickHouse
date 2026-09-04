#include <Parsers/maskSettingValue.h>

#include <Databases/DataLake/DataLakeConstants.h>
#include <Storages/Kafka/Kafka_fwd.h>
#include <Storages/NATS/NATS_fwd.h>
#include <Storages/ObjectStorageQueue/AzureQueue_fwd.h>
#include <Storages/ObjectStorageQueue/S3Queue_fwd.h>
#include <Storages/RabbitMQ/RabbitMQ_fwd.h>
#include <Common/maskURIPassword.h>

#include <functional>

namespace DB
{

namespace
{

/// `format_avro_schema_registry_url` belongs to no engine's registry: it is a format setting, and
/// any engine reading Avro may carry it.
using MaskFunction = std::function<std::string(std::string_view)>;

constexpr std::string_view FORMAT_AVRO_SCHEMA_REGISTRY_URL = "format_avro_schema_registry_url";

std::optional<String> maskAvroSchemaRegistryUrl(std::string_view value)
{
    String masked{value};
    if (!maskURIPassword(&masked))
        return {};
    return masked;
}

/// The registry rule that masks this setting for this engine, or `nullptr` when no registry claims
/// it. Both entry points below go through this, so "is it secret" and "what does it look like
/// masked" cannot drift apart. `format_avro_schema_registry_url` stays outside it: it belongs to no
/// engine, and it masks only when the value actually embeds a password, so it cannot be reduced to
/// a rule that always produces a masked rendering.
const MaskFunction * findMask(std::string_view engine_name, const String & setting_name)
{
    if (auto it = DataLake::SETTINGS_TO_HIDE.find(setting_name); it != DataLake::SETTINGS_TO_HIDE.end())
        return &it->second;

    const auto find_in = [&](std::string_view owner, const auto & registry) -> const MaskFunction *
    {
        if (owner != engine_name)
            return nullptr;
        if (auto it = registry.find(setting_name); it != registry.end())
            return &it->second;
        return nullptr;
    };

    if (const auto * mask = find_in(RabbitMQ::TABLE_ENGINE_NAME, RabbitMQ::SETTINGS_TO_HIDE))
        return mask;
    if (const auto * mask = find_in(NATS::TABLE_ENGINE_NAME, NATS::SETTINGS_TO_HIDE))
        return mask;
    if (const auto * mask = find_in(Kafka::TABLE_ENGINE_NAME, Kafka::SETTINGS_TO_HIDE))
        return mask;
    if (const auto * mask = find_in(AzureQueue::TABLE_ENGINE_NAME, AzureQueue::SETTINGS_TO_HIDE))
        return mask;
    if (const auto * mask = find_in(S3Queue::TABLE_ENGINE_NAME, S3Queue::SETTINGS_TO_HIDE))
        return mask;

    return nullptr;
}

}

bool canMaskSettingValue(std::string_view engine_name, const String & setting_name)
{
    /// True for the Avro URL whether or not this particular value has a password in it: the caller
    /// uses this to decide whether rendering the value is worth it, and only the rendered value can
    /// answer that question.
    return findMask(engine_name, setting_name) != nullptr || setting_name == FORMAT_AVRO_SCHEMA_REGISTRY_URL;
}

std::optional<String> maskSettingValue(std::string_view engine_name, const String & setting_name, std::string_view value)
{
    if (const auto * mask = findMask(engine_name, setting_name))
        return (*mask)(value);

    if (setting_name == FORMAT_AVRO_SCHEMA_REGISTRY_URL)
        return maskAvroSchemaRegistryUrl(value);

    return {};
}

bool isSecretSettingName(const String & setting_name)
{
    return DataLake::SETTINGS_TO_HIDE.contains(setting_name)
        || RabbitMQ::SETTINGS_TO_HIDE.contains(setting_name)
        || NATS::SETTINGS_TO_HIDE.contains(setting_name)
        || Kafka::SETTINGS_TO_HIDE.contains(setting_name)
        || AzureQueue::SETTINGS_TO_HIDE.contains(setting_name)
        || S3Queue::SETTINGS_TO_HIDE.contains(setting_name);
}

}
