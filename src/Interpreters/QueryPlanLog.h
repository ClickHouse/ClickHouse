#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>
#include <base/types.h>

namespace DB
{

struct QueryPlanLogElement
{
    enum Status : int8_t
    {
        QUERY_FINISH = 1,
        EXCEPTION_BEFORE_START = 2,
        EXCEPTION_WHILE_PROCESSING = 3,
    };

    time_t event_time{};
    Decimal64 event_time_microseconds{};
    Decimal64 query_start_time{};

    String query_id;
    String query_string;
    UInt64 query_duration_ms{};
    UInt64 normalized_query_hash{};

    String ascii_plan;
    Status status{};

    static std::string name() { return "QueryPlanLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class QueryPlanLog : public SystemLog<QueryPlanLogElement>
{
public:
    using SystemLog<QueryPlanLogElement>::SystemLog;
};
}
