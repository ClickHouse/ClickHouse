#pragma once

#include <cstddef>

namespace DB
{

struct ColumnSize
{
    size_t marks = 0;
    size_t data_compressed = 0;
    size_t data_uncompressed = 0;

    void add(const ColumnSize & other)
    {
        marks += other.marks;
        data_compressed += other.data_compressed;
        data_uncompressed += other.data_uncompressed;
    }
};

using IndexSize = ColumnSize;

}
