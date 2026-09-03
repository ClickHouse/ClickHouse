#pragma once
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/maskURIPassword.h>

namespace RabbitMQ
{

static constexpr auto TABLE_ENGINE_NAME = "RabbitMQ";
static constexpr auto DEFAULT_MASKING_RULE = [](std::string_view){ return "[HIDDEN]"; };

using ValueMaskingFunc = std::function<std::string(std::string_view)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"rabbitmq_password", DEFAULT_MASKING_RULE},
    {"rabbitmq_address", [](std::string_view value)
    {
        std::string masked_value{value};
        DB::maskURIPassword(&masked_value);
        return masked_value;
    }}
};

}
