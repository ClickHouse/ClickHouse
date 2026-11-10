#pragma once

#include <chrono>
#include <memory>
#include <set>
#include <sys/types.h>

#include <Columns/IColumn_fwd.h>
#include <Core/NamesAndAliases.h>
#include <Core/QueryLogElementType.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>
#include <base/types.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
class Counters;
}

namespace DB
{
class ThreadStatus;

struct QueryViewsLogElement
{
    using ViewStatus = QueryLogElementType;

    enum class ViewType : int8_t
    {
        DEFAULT = 1,
        MATERIALIZED = 2,
        LIVE = 3,
        WINDOW = 4,
    };

    time_t event_time{};
    Decimal64 event_time_microseconds{};
    UInt64 view_duration_ms{};

    String initial_query_id;
    String view_name;
    UUID view_uuid{UUIDHelpers::Nil};
    ViewType view_type{ViewType::DEFAULT};
    String view_query;
    String view_target;
    /// The user the view's query ran as: the definer of a `SQL SECURITY DEFINER` view, empty for `SQL SECURITY NONE`,
    /// otherwise the user of the query.
    String user;
    /// What the view's query accessed. Filled for a view that runs under a query context of its own (`SQL SECURITY
    /// DEFINER` / `NONE`); the objects accessed by a view that runs as the user are in the user's `system.query_log` row.
    std::set<String> databases;
    std::set<String> tables;
    std::set<String> columns;
    std::set<String> partitions;
    std::set<String> projections;
    std::set<String> used_row_policies;

    UInt64 read_rows{};
    UInt64 read_bytes{};
    UInt64 written_rows{};
    UInt64 written_bytes{};
    Int64 peak_memory_usage{};
    std::optional<ProfileEvents::Counters::Snapshot> profile_counters;

    ViewStatus status = ViewStatus::QUERY_START;
    Int32 exception_code{};
    String exception;
    String stack_trace;

    static std::string name() { return "QueryLog"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};


class QueryViewsLog : public SystemLog<QueryViewsLogElement>
{
    using SystemLog<QueryViewsLogElement>::SystemLog;
};

}
