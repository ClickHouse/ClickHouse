#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/HiddenSecret.h>
#include <optional>

namespace S3Queue
{

static constexpr auto TABLE_ENGINE_NAME = "S3Queue";

using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
/// A function rather than a global map: a `static inline` map in a header is constructed by a static
/// initializer in every translation unit that includes it.
inline std::unordered_map<String, ValueMaskingFunc> settingsToHide()
{
    return
    {
        {"after_processing_move_secret_access_key", DB::hideSecretValue},
    };
}

}
