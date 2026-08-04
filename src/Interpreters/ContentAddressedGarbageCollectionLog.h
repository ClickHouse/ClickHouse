#pragma once
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct ContentAddressedGarbageCollectionLogElement
{
    /// `PHASE`: one row per GC phase, emitted between the round's `START` and `FINISH` and correlated
    /// with them by `round_id`.
    enum EventType : int8_t { START = 1, FINISH = 2, PHASE = 3 };
    /// `DEFERRED`: the round acquired the GC lease and took the skip-unchanged fast path -- no fold, no
    /// pre-CAS deletes, no `gc/state` CAS. Kept distinct from `SUCCESS` so a query against this table can
    /// tell a round that genuinely folded and found nothing apart from one that never folded at all.
    enum Outcome   : int8_t { UNKNOWN = 1, SUCCESS = 2, NOT_A_LEADER = 3, FAILED = 4, DEFERRED = 5 };
    enum Trigger   : int8_t { SCHEDULED = 1, MANUAL = 2 };

    time_t event_time = 0;
    Decimal64 event_time_microseconds = 0;

    EventType event_type = START;
    String disk_name;
    String srid;         /// server_root_id of the mount whose GC scheduler ran this round
    String gc_id;
    Trigger trigger = SCHEDULED;

    UInt64 round = 0;
    Outcome outcome = UNKNOWN;      /// UNKNOWN on START; set to SUCCESS/NOT_A_LEADER/FAILED on FINISH
    UInt64 candidates_marked = 0;
    UInt64 objects_deleted = 0;
    UInt64 objects_absent = 0;
    UInt64 objects_replaced = 0;
    UInt64 objects_spared = 0;
    UInt64 manifests_deleted = 0;   /// owner-removed manifest bodies deleted (B11 — distinct from blob deletes)
    UInt64 entries_condemned = 0;   /// retired-cursor pipeline: entries newly condemned this round
    UInt64 entries_graduated = 0;   /// retired-cursor pipeline: entries newly round-passed (delete_pending) this round
    UInt64 entries_redeleted = 0;   /// retired-cursor pipeline: pending exact-token blob deletes executed this round
    UInt64 fence_outs = 0;          /// expired mounts fenced-out by the round's heartbeat floor
    UInt64 anomalies = 0;           /// fold clamps surfaced this round
    UInt64 duration_ms = 0;
    String error;
    std::map<String, UInt64> profile_events;   /// per-round delta (FINISH); per-phase delta (PHASE)

    String round_id;                           /// correlator for every row of one round attempt
    String phase;                              /// empty on START/FINISH
    UInt64 phase_duration_microseconds = 0;              /// PHASE rows only
    std::map<String, UInt64> phase_metrics;    /// PHASE rows only

    static std::string name() { return "ContentAddressedGarbageCollectionLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class ContentAddressedGarbageCollectionLog : public SystemLog<ContentAddressedGarbageCollectionLogElement>
{
    using SystemLog<ContentAddressedGarbageCollectionLogElement>::SystemLog;
};

}
