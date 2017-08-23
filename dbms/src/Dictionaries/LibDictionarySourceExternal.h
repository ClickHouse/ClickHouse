#pragma once

//struct LoadIdsParams {const uint64_t size; const uint64_t * data;};

//using ClickhouseColumns = const char**;
namespace ClickHouseLib
{
using CString = const char *;
using Column = CString;
using Columns = Column[];

struct CStrings
{
    uint64_t size = 0;
    CString * data = nullptr;
};

struct VectorUint64
{
    uint64_t size = 0;
    const uint64_t * data = nullptr;
};

struct ColumnsUint64
{
    uint64_t size = 0;
    VectorUint64 * data = nullptr;
};
}

//using Settings = CStrings;
