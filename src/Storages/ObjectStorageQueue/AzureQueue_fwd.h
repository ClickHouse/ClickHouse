#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/maskURIPassword.h>

namespace AzureQueue
{

static constexpr auto TABLE_ENGINE_NAME = "AzureQueue";
static constexpr auto DEFAULT_MASKING_RULE = [](const DB::Field &){ return "'[HIDDEN]'"; };

using ValueMaskingFunc = std::function<std::string(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"after_processing_move_connection_string", [](const DB::Field & value)
    {
        std::string masked_value = value.safeGet<std::string>();
        DB::maskConnectionStringKey(masked_value, "AccountKey=");
        DB::maskConnectionStringKey(masked_value, "SharedAccessSignature=");
        return fmt::format("'{}'", masked_value);
    }},
};

}
