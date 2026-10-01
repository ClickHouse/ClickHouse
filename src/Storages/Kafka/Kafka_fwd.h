#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/HiddenSecret.h>
#include <optional>

namespace Kafka
{

static constexpr auto TABLE_ENGINE_NAME = "Kafka";

using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
/// Masks the `SETTINGS` clause of the `Kafka` engine, as its `SecretArgumentsSpec::secret_settings`.
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"kafka_sasl_password", DB::hideSecretValue},
};

}
