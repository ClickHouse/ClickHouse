#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/HiddenSecret.h>
#include <optional>

namespace NATS
{

static constexpr auto TABLE_ENGINE_NAME = "NATS";

using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
/// Masks the `SETTINGS` clause of the `NATS` engine, as its `SecretArgumentsSpec::secret_settings`.
/// Keep in sync with `nats_secret_keys` in `StorageNATS.cpp`.
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"nats_password", DB::hideSecretValue},
    {"nats_token", DB::hideSecretValue},
    {"nats_credential_file", DB::hideSecretValue},
    {"nats_credentials", DB::hideSecretValue},
    {"nats_server_list", DB::hideSecretValue},
    {"nats_url", [](const DB::Field & value) -> std::optional<std::string>
    {
        /// A masking rule must not throw: it runs before the setting is validated, so the value
        /// need not be a String. `SETTINGS nats_url = 4222` used to throw `BAD_GET` here, which
        /// aborted the masking of every other setting in the same statement.
        std::string masked_value;
        if (!value.tryGet<std::string>(masked_value))
            return {};
        /// libnats takes the scheme as optional and ends the userinfo at the LAST '@' of the whole
        /// value (`contrib/nats-io/src/url.c`, `natsUrl_Create`), so no URI authority bounds it.
        if (masked_value.contains('@'))
            masked_value = DB::HIDDEN_SECRET;
        return fmt::format("'{}'", masked_value);
    }}
};

}
