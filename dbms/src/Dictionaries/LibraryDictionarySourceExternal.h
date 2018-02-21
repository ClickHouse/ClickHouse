#pragma once

#include <cstdint>

#define CLICKHOUSE_DICTIONARY_LIBRARY_API 1

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

struct Field
{
    const void * data = nullptr;
    uint64_t size = 0;
};

struct Row
{
    const Field * data = nullptr;
    uint64_t size = 0;
};

struct Table
{
    const Row * data = nullptr;
    uint64_t size = 0;
    uint64_t error_code = 0; // 0 = ok; !0 = error, with message in error_string
    const char * error_string = nullptr;
};
}
