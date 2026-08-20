#pragma once
#include <Core/Types.h>
#include <Core/Field.h>

namespace S3Queue
{

static constexpr auto TABLE_ENGINE_NAME = "S3Queue";
static constexpr auto DEFAULT_MASKING_RULE = [](const DB::Field &){ return "'[HIDDEN]'"; };

using ValueMaskingFunc = std::function<std::string(const DB::Field &)>;
static inline std::unordered_map<String, ValueMaskingFunc> SETTINGS_TO_HIDE =
{
    {"after_processing_move_secret_access_key", DEFAULT_MASKING_RULE},
};

}
