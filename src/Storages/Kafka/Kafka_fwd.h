#pragma once
#include <Core/Types.h>
#include <Core/Field.h>

namespace Kafka
{

static constexpr auto TABLE_ENGINE_NAME = "Kafka";
static constexpr auto DEFAULT_MASKING_RULE = [](std::string_view){ return "[HIDDEN]"; };

using ValueMaskingFunc = std::function<std::string(std::string_view)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"kafka_sasl_password", DEFAULT_MASKING_RULE},
};

}
