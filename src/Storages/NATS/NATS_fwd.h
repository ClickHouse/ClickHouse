#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <optional>

namespace NATS
{

static constexpr auto TABLE_ENGINE_NAME = "NATS";
static constexpr auto DEFAULT_MASKING_RULE = [](const DB::Field &){ return "[HIDDEN]"; };

using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"nats_password", DEFAULT_MASKING_RULE},
    {"nats_token", DEFAULT_MASKING_RULE},
    {"nats_credential_file", DEFAULT_MASKING_RULE},
    {"nats_credentials", DEFAULT_MASKING_RULE},
    {"nats_server_list", DEFAULT_MASKING_RULE},
    {"nats_url", [](const DB::Field & value) -> std::optional<std::string>
    {
        /// A masking rule must not throw: it runs before the setting is validated, so the value
        /// need not be a String. `SETTINGS nats_url = 4222` used to throw `BAD_GET` here, which
        /// aborted the masking of every other setting in the same statement.
        std::string masked_value;
        if (!value.tryGet<std::string>(masked_value))
            return {};
        /// libnats takes the scheme as optional and ends the userinfo at the LAST '@' of the whole
        /// value (`contrib/nats-io/src/url.c`, `natsUrl_Create`), so no URI authority bounds it and
        /// nothing short of the whole value can be masked safely.
        if (masked_value.contains('@'))
            masked_value = "[HIDDEN]";
        return masked_value;
    }}
};

}
