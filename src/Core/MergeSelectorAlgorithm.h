#pragma once

#include <cstdint>
namespace DB
{

enum class MergeSelectorAlgorithm : uint8_t
{
    SIMPLE,
    STOCHASTIC_SIMPLE,
    TRIVIAL,
};

}
