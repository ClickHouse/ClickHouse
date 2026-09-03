#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/maskURIPassword.h>

namespace NATS
{

static constexpr auto TABLE_ENGINE_NAME = "NATS";
static constexpr auto DEFAULT_MASKING_RULE = [](std::string_view){ return "[HIDDEN]"; };

using ValueMaskingFunc = std::function<std::string(std::string_view)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"nats_password", DEFAULT_MASKING_RULE},
    {"nats_token", DEFAULT_MASKING_RULE},
    {"nats_credential_file", DEFAULT_MASKING_RULE},
    {"nats_credentials", DEFAULT_MASKING_RULE},
    {"nats_server_list", DEFAULT_MASKING_RULE},
    {"nats_url", [](std::string_view value)
    {
        std::string masked_value{value};
        DB::maskURIPassword(&masked_value);
        return masked_value;
    }}
};

}
