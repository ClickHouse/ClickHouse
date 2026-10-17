#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/HiddenSecret.h>
#include <optional>

namespace S3Queue
{

static constexpr auto TABLE_ENGINE_NAME = "S3Queue";

using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"after_processing_move_secret_access_key", DB::hideSecretValue},
};

}
