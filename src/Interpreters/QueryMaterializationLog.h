#pragma once

#include <memory>
#include <sys/types.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SettingsEnums.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Interpreters/SystemLog.h>
#include <common/types.h>

namespace ProfileEvents
{
class Counters;
}

namespace DB
{
struct QueryMaterializationLogElement
{
    using Status = QueryLogElementType;

    time_t event_time{};
    Decimal64 event_time_microseconds{};
    time_t materialization_start_time{};
    Decimal64 materialization_start_time_microseconds{};
    UInt64 materialization_duration_ms{};

    String initial_query_id;
    String materialization_name;
    UUID materialization_uuid{UUIDHelpers::Nil};
    String materialization_query;

    UInt64 read_rows{};
    UInt64 read_bytes{};
    UInt64 written_rows{};
    UInt64 written_bytes{};
    Int64 memory_usage{};
    Int64 peak_memory_usage{};
    std::shared_ptr<ProfileEvents::Counters> profile_counters;

    Status end_status{EXCEPTION_BEFORE_START};
    Int32 exception_code{};
    String exception;
    String stack_trace;

    static std::string name() { return "QueryMutationLog"; }

    static Block createBlock();
    void appendToBlock(MutableColumns & columns) const;
};


class QueryMaterializationLog : public SystemLog<QueryMaterializationLogElement>
{
    using SystemLog<QueryMaterializationLogElement>::SystemLog;
};

}
