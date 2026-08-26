#pragma once
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>
#include <map>

namespace DB
{

/// One row per content-addressed (CA) decision/event (B170). The decoupled Core POD `Cas::CasEvent`
/// is mapped to this element by `ContentAddressedMetadataStorage::makeCasEventSink` and forwarded to
/// the SystemLog. Optional (off by default); enabled for soak/CI. The set is exhaustive enough to
/// reconstruct an entity's whole lifetime; `reason`/`detail` carry each decision's rationale.
struct ContentAddressedLogElement
{
    time_t event_time = 0;
    Decimal64 event_time_microseconds = 0;

    String event_type;            /// Cas::CasEventType name (snake_case), LowCardinality in the table
    String disk_name;
    String namespace_;
    String ref_name;
    String object_kind;           /// none/blob/manifest/root/snap
    String object_hash;
    String token;
    UInt64 round = 0;
    UInt64 gen = 0;
    UInt64 at_version = 0;
    String outcome;
    String reason;
    UInt64 thread_id = 0;
    String query_id;
    std::map<String, String> detail;

    static std::string name() { return "ContentAddressedLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class ContentAddressedLog : public SystemLog<ContentAddressedLogElement>
{
    using SystemLog<ContentAddressedLogElement>::SystemLog;
};

}
