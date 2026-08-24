#pragma once

#include <cstdint>

namespace DB
{

enum class ShortCircuitFunctionEvaluation : uint8_t
{
    ENABLE, // Use short-circuit function evaluation for functions that are suitable for it.
    FORCE_ENABLE, // Use short-circuit function evaluation for all functions.
    DISABLE, // Disable short-circuit function evaluation.
};

}
