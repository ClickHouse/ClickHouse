#pragma once

#include <cstdint>
#include <string>

#define CLICKHOUSE_DICTIONARY_LIBRARY_API 1

struct ExternalDictionaryLibraryAPI
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

    static void log(LogLevel level, CString msg);

    using LibraryContext = void *;
    using LibraryLoggerFunc = void (*)(LogLevel, CString /* message */);
    using LibrarySettings = CStrings *;
    using LibraryData = void *;
    using RawClickHouseLibraryTable = void *;
    /// Can be safely casted into const Table * with static_cast<const ClickHouseLibrary::Table *>
    using RequestedColumnsNames = CStrings *;
    using RequestedIds = const VectorUInt64 *;
    using RequestedKeys = Table *;

    using LibraryNewFunc = LibraryContext (*)(LibrarySettings, LibraryLoggerFunc);
    static constexpr const char * LIBRARY_CREATE_NEW_FUNC_NAME = "ClickHouseDictionary_v3_libNew";

    using LibraryCloneFunc = LibraryContext (*)(LibraryContext);
    static constexpr const char * LIBRARY_CLONE_FUNC_NAME = "ClickHouseDictionary_v3_libClone";

    using LibraryDeleteFunc = void (*)(LibraryContext);
    static constexpr const char * LIBRARY_DELETE_FUNC_NAME = "ClickHouseDictionary_v3_libDelete";

    using LibraryDataNewFunc = LibraryData (*)(LibraryContext);
    static constexpr const char * LIBRARY_DATA_NEW_FUNC_NAME = "ClickHouseDictionary_v3_dataNew";

    using LibraryDataDeleteFunc = void (*)(LibraryContext, LibraryData);
    static constexpr const char * LIBRARY_DATA_DELETE_FUNC_NAME = "ClickHouseDictionary_v3_dataDelete";

    using LibraryLoadAllFunc = RawClickHouseLibraryTable (*)(LibraryData, LibrarySettings, RequestedColumnsNames);
    static constexpr const char * LIBRARY_LOAD_ALL_FUNC_NAME = "ClickHouseDictionary_v3_loadAll";

    using LibraryLoadIdsFunc = RawClickHouseLibraryTable (*)(LibraryData, LibrarySettings, RequestedColumnsNames, RequestedIds);
    static constexpr const char * LIBRARY_LOAD_IDS_FUNC_NAME = "ClickHouseDictionary_v3_loadIds";

    /// There are no requested column names for load keys func
    using LibraryLoadKeysFunc = RawClickHouseLibraryTable (*)(LibraryData, LibrarySettings, RequestedKeys);
    static constexpr const char * LIBRARY_LOAD_KEYS_FUNC_NAME = "ClickHouseDictionary_v3_loadKeys";

    using LibraryIsModifiedFunc = bool (*)(LibraryContext, LibrarySettings);
    static constexpr const char * LIBRARY_IS_MODIFIED_FUNC_NAME = "ClickHouseDictionary_v3_isModified";

    using LibrarySupportsSelectiveLoadFunc = bool (*)(LibraryContext, LibrarySettings);
    static constexpr const char * LIBRARY_SUPPORTS_SELECTIVE_LOAD_FUNC_NAME = "ClickHouseDictionary_v3_supportsSelectiveLoad";
};
