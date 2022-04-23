#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/TransactionVersionMetadata.h>

namespace DB
{

struct CacheLogRecorder
{
public:
    std::atomic<size_t> cache_hit_count = 0;
    std::atomic<size_t> cache_miss_count = 0;

public:
    void incrementHitsCount() { cache_hit_count++; }
    void incrementMissCount() { cache_miss_count++; }
};

struct CacheLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};

    String query_id;

    UInt64 hit_count;
    UInt64 miss_count;

    /// Float64 hit_ratio;
    /// Float64 miss_ratio;

    /// TODO for next version
    /// std::unordered_map<String, size_t> hit_records;
    /// std::unordered_map<String, size_t> miss_records;

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
