#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/TransactionVersionMetadata.h>

namespace DB
{

struct CacheLogElement
{
    String current_query_id;

    Float64 hit_ratio;
    Float64 miss_ratio;

    UInt64 hit_count;
    UInt64 miss_count;

    std::unordered_map<String, size_t> hit_records;
    std::unordered_map<String, size_t> miss_records;

    static std::string name() { return "CacheLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
    static void appendClientInfo(const ClientInfo & client_info, MutableColumns & columns, size_t & i);
};

class QueryLog : public SystemLog<CacheLogElement>
{
    using SystemLog<CacheLogElement>::SystemLog;
};

};
