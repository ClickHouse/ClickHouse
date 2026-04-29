#pragma once

#include <cstddef>

namespace DB
{

struct Range
{
    size_t offset = 0;
    size_t size = 0;
    size_t end() const { return offset + size; }
};

}
