#pragma once

#include <cstdint>
#include <string>

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

enum LogLevel
{
    FATAL = 1,
    CRITICAL,
    ERROR,
    WARNING,
    NOTICE,
    INFORMATION,
    DEBUG,
    TRACE,
};

void log(LogLevel level, CString msg);

extern std::string_view LIBRARY_CREATE_NEW_FUNC_NAME;
extern std::string_view LIBRARY_CLONE_FUNC_NAME;
extern std::string_view LIBRARY_DELETE_FUNC_NAME;

extern std::string_view LIBRARY_DATA_NEW_FUNC_NAME;
extern std::string_view LIBRARY_DATA_DELETE_FUNC_NAME;

extern std::string_view LIBRARY_LOAD_ALL_FUNC_NAME;
extern std::string_view LIBRARY_LOAD_IDS_FUNC_NAME;
extern std::string_view LIBRARY_LOAD_KEYS_FUNC_NAME;

extern std::string_view LIBRARY_IS_MODIFIED_FUNC_NAME;
extern std::string_view LIBRARY_SUPPORTS_SELECTIVE_LOAD_FUNC_NAME;

using LibraryContext = void *;

using LibraryLoggerFunc = void (*)(LogLevel, CString /* message */);

using LibrarySettings = CStrings *;

using LibraryNewFunc = LibraryContext (*)(LibrarySettings, LibraryLoggerFunc);
using LibraryCloneFunc = LibraryContext (*)(LibraryContext);
using LibraryDeleteFunc = void (*)(LibraryContext);

using LibraryData = void *;
using LibraryDataNewFunc = LibraryData (*)(LibraryContext);
using LibraryDataDeleteFunc = void (*)(LibraryContext, LibraryData);

/// Can be safely casted into const Table * with static_cast<const ClickHouseLibrary::Table *>
using RawClickHouseLibraryTable = void *;
using RequestedColumnsNames = CStrings *;

using LibraryLoadAllFunc = RawClickHouseLibraryTable (*)(LibraryData, LibrarySettings, RequestedColumnsNames);

using RequestedIds = const VectorUInt64 *;
using LibraryLoadIdsFunc = RawClickHouseLibraryTable (*)(LibraryData, LibrarySettings, RequestedColumnsNames, RequestedIds);

using RequestedKeys = Table *;
/// There are no requested column names for load keys func
using LibraryLoadKeysFunc = RawClickHouseLibraryTable (*)(LibraryData, LibrarySettings, RequestedKeys);

using LibraryIsModifiedFunc = bool (*)(LibraryContext, LibrarySettings);
using LibrarySupportsSelectiveLoadFunc = bool (*)(LibraryContext, LibrarySettings);

}
