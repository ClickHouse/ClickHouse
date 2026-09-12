#include <Storages/maskEngineSettingValue.h>

#include <Core/Field.h>
#include <Core/SettingsSecrets.h>
#include <Databases/DataLake/DataLakeConstants.h>
#include <Storages/Kafka/Kafka_fwd.h>
#include <Storages/NATS/NATS_fwd.h>
#include <Storages/ObjectStorageQueue/AzureQueue_fwd.h>
#include <Storages/ObjectStorageQueue/S3Queue_fwd.h>
#include <Storages/RabbitMQ/RabbitMQ_fwd.h>

#include <array>
#include <optional>
#include <functional>
#include <unordered_map>

namespace DB
{

bool maskEngineSettingValue(const String & setting_name, const Field & field, String & value)
{
    /// A core setting can appear in a table's settings too - `format_avro_schema_registry_url` is a
    /// format setting any engine reading Avro may carry - and this also covers a value that is an
    /// AST rather than a literal.
    if (CoreSettings::maskSettingValue(setting_name, field, value))
        return true;

    /// Each engine namespace declares its own identical `ValueMaskingFunc`, hence the spelled-out
    /// type - the same one `ASTSetQuery` spells as `EngineSettingsToHide`.
    using SettingsToHide = std::unordered_map<String, std::function<std::optional<std::string>(const Field &)>>;
    static const std::array<const SettingsToHide *, 6> registries{
        &DataLake::SETTINGS_TO_HIDE,
        &RabbitMQ::SETTINGS_TO_HIDE,
        &NATS::SETTINGS_TO_HIDE,
        &Kafka::SETTINGS_TO_HIDE,
        &AzureQueue::SETTINGS_TO_HIDE,
        &S3Queue::SETTINGS_TO_HIDE,
    };

    for (const auto * registry : registries)
    {
        if (auto it = registry->find(setting_name); it != registry->end())
        {
            /// A registry names a setting that *may* carry a secret and decides per value whether
            /// this one does - a URL without credentials in it does not - so `nullopt` means there
            /// is nothing to hide. The first registry that knows the name answers, as
            /// `ASTSetQuery::renderSecretChangeValue` does, so the two cannot disagree on what is
            /// secret.
            auto rendered = it->second(field);
            if (!rendered)
                return false;

            /// The registries render the SQL literal, quotes included, because their other caller
            /// prints SQL. A column prints the value itself, so take the quotes back off.
            if (rendered->size() >= 2 && rendered->front() == '\'' && rendered->back() == '\'')
                *rendered = rendered->substr(1, rendered->size() - 2);
            value = std::move(*rendered);
            return true;
        }
    }

    return false;
}

}
