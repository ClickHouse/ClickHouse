#pragma once

#include <cstdint>

namespace DB
{

enum class AsynchronousInsertQueueDataKind : uint8_t
{
    Parsed = 0,
    Preprocessed = 1,
};

}
