#pragma once

#include <cstdint>

namespace ClickHouseLibrary
{
using CString = const char *;
using ColumnName = CString;
using ColumnNames = ColumnName[];

struct CStrings
{
    CString * data = nullptr;
    uint64_t size = 0;
};

struct VectorUInt64
{
    const uint64_t * data = nullptr;
    uint64_t size = 0;
};

struct ColumnsUInt64
{
    VectorUInt64 * data = nullptr;
    uint64_t size = 0;
};
}
