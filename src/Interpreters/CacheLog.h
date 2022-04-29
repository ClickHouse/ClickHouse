#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <base/logger_useful.h>

namespace DB
{
struct CacheLogElement
{
    time_t event_time{};
    String query_id;
    String remote_file_path;

    UInt64 hit_count = 0;
    UInt64 miss_count = 0;

    static std::string name() { return "CacheLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

/// [remote_file_path, [hit_count, miss_count]]
using CacheLogElementPtr = std::shared_ptr<CacheLogElement>;
using CacheFileTrace = std::unordered_map<String, CacheLogElementPtr>;
using CacheFileTracePtr = std::shared_ptr<CacheFileTrace>;

struct CacheLogRecorder
{
    size_t ref = 0;
    CacheFileTracePtr trace;
};

class CacheLog : public SystemLog<CacheLogElement>
{
    using SystemLog<CacheLogElement>::SystemLog;
};

};
