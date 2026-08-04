#include <Interpreters/ContentAddressedLog.h>
#include <base/getFQDNOrHostName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/DateLUTImpl.h>

namespace DB
{

ColumnsDescription ContentAddressedLogElement::getColumnsDescription()
{
    auto lc_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    return ColumnsDescription
    {
        {"hostname", lc_string, "Host name of the server that emitted the event."},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds."},
        {"event_type", lc_string, "The CA decision/event (blob_put, blob_reuse_adopt, root_remove, indegree_zero, gc_retire_decision, gc_recheck_verdict, blob_delete, dangling_access, corrupt_dangle, ...)."},
        {"disk_name", lc_string, "Content-addressed disk / pool the event belongs to."},
        {"namespace", std::make_shared<DataTypeString>(), "roots/<namespace> (server/table), empty if N/A."},
        {"ref_name", std::make_shared<DataTypeString>(), "Part name / ref the event concerns, empty if N/A."},
        {"object_kind", lc_string, "none/blob/manifest/root/snapshot."},
        {"object_hash", std::make_shared<DataTypeString>(), "Content hash (lowercase hex) of the object, empty if N/A."},
        {"token", std::make_shared<DataTypeString>(), "Incarnation token (ETag) involved, empty if N/A."},
        {"round", std::make_shared<DataTypeUInt64>(), "GC round (0 if N/A)."},
        {"generation", std::make_shared<DataTypeUInt64>(), "GC snapshot generation (0 if N/A)."},
        {"at_version", std::make_shared<DataTypeUInt64>(), "Manifest shard_version of the driving journal record (0 if N/A)."},
        {"outcome", lc_string, "Decision outcome (ok/adopt/resurrect/deleted/replaced/spared/absent/zeroed/skipped/...)."},
        {"reason", lc_string, "Human-readable WHY of the decision (the rationale) -- templated across rows, so LowCardinality."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "OS thread that emitted the event."},
        {"query_id", std::make_shared<DataTypeString>(), "Query id for correlation with system.query_log (empty if N/A)."},
        {"detail", std::make_shared<DataTypeMap>(lc_string, std::make_shared<DataTypeString>()),
            "Structured event-specific facts (e.g. condemn_round, superseded_token, code, site)."},
    };
}

void ContentAddressedLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(event_type);
    columns[i++]->insert(disk_name);
    columns[i++]->insert(namespace_);
    columns[i++]->insert(ref_name);
    columns[i++]->insert(object_kind);
    columns[i++]->insert(object_hash);
    columns[i++]->insert(token);
    columns[i++]->insert(round);
    columns[i++]->insert(gen);
    columns[i++]->insert(at_version);
    columns[i++]->insert(outcome);
    columns[i++]->insert(reason);
    columns[i++]->insert(thread_id);
    columns[i++]->insert(query_id);
    {
        Map map;
        map.reserve(detail.size());
        for (const auto & [k, v] : detail)
            map.push_back(Tuple{k, v});
        columns[i++]->insert(map);
    }
}

}
