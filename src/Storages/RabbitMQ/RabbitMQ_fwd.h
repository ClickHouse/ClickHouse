#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/maskURIPassword.h>

namespace RabbitMQ
{

static constexpr auto TABLE_ENGINE_NAME = "RabbitMQ";
static constexpr auto DEFAULT_MASKING_RULE = [](const DB::Field &){ return "'[HIDDEN]'"; };

using ValueMaskingFunc = std::function<std::string(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"rabbitmq_password", DEFAULT_MASKING_RULE},
    {"rabbitmq_address", [](const DB::Field & value)
    {
        std::string masked_value = value.safeGet<std::string>();
        DB::maskURIPassword(&masked_value);
        return fmt::format("'{}'", masked_value);
    }}
};

}
