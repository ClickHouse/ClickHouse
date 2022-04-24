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

/// [remote_file_path, [hit_count, miss_count]]
using CacheFileTrace = std::unordered_map<String, std::pair<size_t, size_t>>;

struct CacheLogRecorder
{
public:
    size_t ref = 0;
    std::shared_ptr<CacheFileTrace> logs;
};

struct CacheLogElement
{
    /// time_t event_time{};
    String query_id;
    String remote_file_path;

    UInt64 hit_count;
    UInt64 miss_count;

    static std::string name() { return "CacheLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};

class CacheLog : public SystemLog<CacheLogElement>
{
    using SystemLog<CacheLogElement>::SystemLog;
};

};
