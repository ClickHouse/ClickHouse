#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/maskURIPassword.h>
#include <optional>

namespace RabbitMQ
{

static constexpr auto TABLE_ENGINE_NAME = "RabbitMQ";
static constexpr auto DEFAULT_MASKING_RULE = [](const DB::Field &){ return "'[HIDDEN]'"; };

using ValueMaskingFunc = std::function<std::optional<std::string>(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"rabbitmq_password", DEFAULT_MASKING_RULE},
    {"rabbitmq_address", [](const DB::Field & value) -> std::optional<std::string>
    {
        /// Not a String means there is nothing to mask; see the `nats_url` rule for why a rule
        /// must not throw.
        std::string masked_value;
        if (!value.tryGet<std::string>(masked_value))
            return {};
        DB::maskURIPassword(&masked_value);
        return fmt::format("'{}'", masked_value);
    }}
};

}
