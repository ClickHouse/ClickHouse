#pragma once

#include <Core/Field.h>
#include <Core/Types.h>

#include <functional>
#include <string>
#include <unordered_map>

namespace ElasticsearchQueue
{

static constexpr auto TABLE_ENGINE_NAME = "ElasticsearchQueue";
static constexpr auto DEFAULT_MASKING_RULE = [](const DB::Field &) { return "'[HIDDEN]'"; };

using ValueMaskingFunc = std::function<std::string(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"elasticsearch_password", DEFAULT_MASKING_RULE},
    {"elasticsearch_api_key", DEFAULT_MASKING_RULE},
    {"elasticsearch_bearer_token", DEFAULT_MASKING_RULE},
};

}
