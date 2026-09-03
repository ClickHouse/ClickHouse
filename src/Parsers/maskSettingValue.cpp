#include <Parsers/maskSettingValue.h>

#include <Databases/DataLake/DataLakeConstants.h>
#include <Storages/Kafka/Kafka_fwd.h>
#include <Storages/NATS/NATS_fwd.h>
#include <Storages/ObjectStorageQueue/AzureQueue_fwd.h>
#include <Storages/ObjectStorageQueue/S3Queue_fwd.h>
#include <Storages/RabbitMQ/RabbitMQ_fwd.h>
#include <Common/maskURIPassword.h>

namespace DB
{

namespace
{

/// `format_avro_schema_registry_url` belongs to no engine's registry: it is a format setting, and
/// any engine reading Avro may carry it.
constexpr std::string_view FORMAT_AVRO_SCHEMA_REGISTRY_URL = "format_avro_schema_registry_url";

std::optional<String> maskAvroSchemaRegistryUrl(std::string_view value)
{
    String masked{value};
    if (!maskURIPassword(&masked))
        return {};
    return masked;
}

}

std::optional<String> maskSettingValue(std::string_view engine_name, const String & setting_name, std::string_view value)
{
    if (auto it = DataLake::SETTINGS_TO_HIDE.find(setting_name); it != DataLake::SETTINGS_TO_HIDE.end())
        return it->second(value);

    const auto mask_for = [&](std::string_view owner, const auto & registry) -> std::optional<String>
    {
        if (owner != engine_name)
            return {};
        if (auto it = registry.find(setting_name); it != registry.end())
            return it->second(value);
        return {};
    };

    if (auto masked = mask_for(RabbitMQ::TABLE_ENGINE_NAME, RabbitMQ::SETTINGS_TO_HIDE))
        return masked;
    if (auto masked = mask_for(NATS::TABLE_ENGINE_NAME, NATS::SETTINGS_TO_HIDE))
        return masked;
    if (auto masked = mask_for(Kafka::TABLE_ENGINE_NAME, Kafka::SETTINGS_TO_HIDE))
        return masked;
    if (auto masked = mask_for(AzureQueue::TABLE_ENGINE_NAME, AzureQueue::SETTINGS_TO_HIDE))
        return masked;
    if (auto masked = mask_for(S3Queue::TABLE_ENGINE_NAME, S3Queue::SETTINGS_TO_HIDE))
        return masked;

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
