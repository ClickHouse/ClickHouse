#pragma once

namespace ClickHouseLibrary
{
using CString = const char *;
using ColumnName = CString;
using ColumnNames = ColumnName[];

struct CStrings
{
    uint64_t size = 0;
    CString * data = nullptr;
};

struct VectorUInt64
{
    uint64_t size = 0;
    const uint64_t * data = nullptr;
};

struct ColumnsUInt64
{
    uint64_t size = 0;
    VectorUInt64 * data = nullptr;
};
}
