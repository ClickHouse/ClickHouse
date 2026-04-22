#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/maskURIPassword.h>

namespace NATS
{

static constexpr auto TABLE_ENGINE_NAME = "NATS";
static constexpr auto DEFAULT_MASKING_RULE = [](const DB::Field &){ return "'[HIDDEN]'"; };

using ValueMaskingFunc = std::function<std::string(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"nats_password", DEFAULT_MASKING_RULE},
    {"nats_credential_file", DEFAULT_MASKING_RULE},
    {"nats_url", [](const DB::Field & value)
    {
        std::string masked_value = value.safeGet<std::string>();
        DB::maskURIPassword(&masked_value);
        return fmt::format("'{}'", masked_value);
    }}
};

}
