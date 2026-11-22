#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/maskURIPassword.h>
#include <optional>

namespace AzureQueue
{

static constexpr auto TABLE_ENGINE_NAME = "AzureQueue";

using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
/// A function rather than a global map: a `static inline` map in a header is constructed by a static
/// initializer in every translation unit that includes it.
inline std::unordered_map<String, ValueMaskingFunc> settingsToHide()
{
    return
    {
        {"after_processing_move_connection_string", [](const DB::Field & value) -> std::optional<std::string>
        {
            /// Not a String means there is nothing to mask; see the `nats_url` rule for why a rule
            /// must not throw.
            std::string masked_value;
            if (!value.tryGet<std::string>(masked_value))
                return {};
            DB::maskAzureConnectionStringSecrets(masked_value);
            return fmt::format("'{}'", masked_value);
        }},
    };
}

}
