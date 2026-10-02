#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/HiddenSecret.h>
#include <optional>

namespace RabbitMQ
{

static constexpr auto TABLE_ENGINE_NAME = "RabbitMQ";

using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
/// Masks the `SETTINGS` clause of the `RabbitMQ` engine, as its `SecretArgumentsSpec::secret_settings`, and the same
/// settings given as overrides of a named collection among the engine arguments.
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"rabbitmq_password", DB::hideSecretValue},
    {"rabbitmq_address", [](const DB::Field & value) -> std::optional<std::string>
    {
        /// Not a String means there is nothing to mask; see the `nats_url` rule for why a rule
        /// must not throw.
        std::string masked_value;
        if (!value.tryGet<std::string>(masked_value))
            return {};
        /// AMQP-CPP ends the login at the FIRST '@' after the scheme, unbounded by the `/?#` that closes
        /// an RFC 3986 authority (`contrib/AMQP-CPP/include/amqpcpp/address.h`), so no URI masker bounds it.
        if (masked_value.contains('@'))
            masked_value = DB::HIDDEN_SECRET;
        return fmt::format("'{}'", masked_value);
    }}
};

}
