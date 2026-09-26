#pragma once

#include <cstdint>


namespace DB
{

/// Logical source of rows counted by an insert pipeline.
enum class InsertSource : uint8_t
{
    Direct,
    MaterializedView,
};

}
