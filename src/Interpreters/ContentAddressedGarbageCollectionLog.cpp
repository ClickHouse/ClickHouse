#include <Interpreters/ContentAddressedGarbageCollectionLog.h>
#include <base/getFQDNOrHostName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/DateLUTImpl.h>

namespace DB
{

ColumnsDescription ContentAddressedGarbageCollectionLogElement::getColumnsDescription()
{
    auto type_enum = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Start", static_cast<Int8>(START)}, {"Finish", static_cast<Int8>(FINISH)},
        {"Phase", static_cast<Int8>(PHASE)}});
    auto outcome_enum = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Unknown", static_cast<Int8>(UNKNOWN)}, {"Success", static_cast<Int8>(SUCCESS)},
        {"NotALeader", static_cast<Int8>(NOT_A_LEADER)}, {"Error", static_cast<Int8>(FAILED)},
        {"Deferred", static_cast<Int8>(DEFERRED)}});
    auto trigger_enum = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Scheduled", static_cast<Int8>(SCHEDULED)}, {"Manual", static_cast<Int8>(MANUAL)}});
    auto lc_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"hostname", lc_string, "Host name of the server executing the round."},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds."},
        {"event_type", type_enum, "Start or Finish of a GC round, or one Phase of it."},
        {"disk_name", lc_string, "Content-addressed disk the round ran on."},
        {"server_root_id", lc_string, "Identifies the mount whose GC scheduler ran this round. Distinguishes concurrent mounters of the same shared pool; join on this column when correlating rounds against `system.cas_mounts`."},
        {"gc_id", std::make_shared<DataTypeString>(), "GC scheduler instance id (which mounter)."},
        {"trigger", trigger_enum, "Scheduled (background tick) or Manual (SYSTEM command)."},
        {"round", std::make_shared<DataTypeUInt64>(), "GC round number (0 on Start)."},
        {"outcome", outcome_enum, "Unknown (Start) / Success (led, folded, and completed) / NotALeader (another replica holds the GC lease) / Deferred (led but took the skip-unchanged fast path -- no fold ran) / Error (the round threw)."},
        {"candidates_marked", std::make_shared<DataTypeUInt64>(), "Objects retired (marked) this round."},
        {"objects_deleted", std::make_shared<DataTypeUInt64>(), "Objects physically deleted this round."},
        {"objects_absent", std::make_shared<DataTypeUInt64>(), "Retire candidates found already absent."},
        {"objects_replaced", std::make_shared<DataTypeUInt64>(), "412-saves (a resurrection won the race)."},
        {"objects_spared", std::make_shared<DataTypeUInt64>(), "Candidates spared (in-degree > 0 at recheck)."},
        {"manifests_deleted", std::make_shared<DataTypeUInt64>(), "Owner-removed manifest bodies physically deleted this round (counted separately from blob deletes, B11)."},
        {"entries_condemned", std::make_shared<DataTypeUInt64>(), "Retired entries newly condemned this round (retired-cursor pipeline stage 1)."},
        {"entries_graduated", std::make_shared<DataTypeUInt64>(), "Retired entries newly floor-passed and republished delete_pending this round (stage 2; deleted the NEXT round)."},
        {"entries_redeleted", std::make_shared<DataTypeUInt64>(), "Pending exact-token blob deletes executed this round (stage 3)."},
        {"fence_outs", std::make_shared<DataTypeUInt64>(), "Expired mounts fenced out by this round's heartbeat floor."},
        {"anomalies", std::make_shared<DataTypeUInt64>(), "Fold clamps surfaced (and survived) this round; steady >0 warrants a look at the round log details."},
        {"duration_ms", std::make_shared<DataTypeUInt64>(), "Round wall-clock duration (Finish)."},
        {"error", std::make_shared<DataTypeString>(), "Exception text when outcome = Error."},
        {"ProfileEvents", std::make_shared<DataTypeMap>(lc_string, std::make_shared<DataTypeUInt64>()),
            "On a Start/Finish row: the per-round ProfileEvents delta (the Cas* counters and S3 events for this round). On a Phase row: THAT PHASE's delta, so `GROUP BY phase` over `ProfileEvents['S3ListObjects']` attributes the round's LIST budget to the phase that spent it. Empty on the `meta_pool_wait` row by construction — that phase's work runs on other threads (read its `phase_metrics` instead)."},
        {"round_id", std::make_shared<DataTypeString>(),
            "Correlator for every row of one round attempt (its Start, each Phase, and its Finish). Minted per attempt; unlike `round` it exists even for a round that never committed and for a round that never led. Group by this column to reconstruct one round."},
        {"phase", lc_string,
            "The GC phase this row describes (empty on Start/Finish), in execution order: lease, pre_fold_ref_drain, heartbeat_floor, defer_decision, parent_seal_read, fold_ref_group, fold_seal_read, fold_ref_intake, fold_reduce, fold_seal_write, pending_deletes, meta_pool_wait, round_commit, handoff_reclaim, manifest_deletes, namespace_cleanup, ref_object_cleanup, orphan_sweep. A round that defers, or that never acquires the lease, emits only the phases it reached."},
        {"phase_duration_microseconds", std::make_shared<DataTypeUInt64>(),
            "Wall-clock duration of this phase in microseconds (Phase rows only). Microseconds because several phases are routinely sub-millisecond and the point is to see when they are not. Phase durations do not sum to the round's `duration_ms`: the round also does untimed bookkeeping between phases."},
        {"phase_metrics", std::make_shared<DataTypeMap>(lc_string, std::make_shared<DataTypeUInt64>()),
            "Phase-specific semantic counts a phase computes for itself and no ProfileEvent can supply (Phase rows only) — for example `changed_shards` on defer_decision, `logs_accounted`/`logs_applied` on fold_ref_intake, `transactions_unapplied` on fold_reduce, `jobs_scheduled`/`jobs_completed` on meta_pool_wait. The verb counts ride the `ProfileEvents` column of the same row."},
    };
}

void ContentAddressedGarbageCollectionLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(static_cast<Int8>(event_type));
    columns[i++]->insert(disk_name);
    columns[i++]->insert(srid);
    columns[i++]->insert(gc_id);
    columns[i++]->insert(static_cast<Int8>(trigger));
    columns[i++]->insert(round);
    columns[i++]->insert(static_cast<Int8>(outcome));
    columns[i++]->insert(candidates_marked);
    columns[i++]->insert(objects_deleted);
    columns[i++]->insert(objects_absent);
    columns[i++]->insert(objects_replaced);
    columns[i++]->insert(objects_spared);
    columns[i++]->insert(manifests_deleted);
    columns[i++]->insert(entries_condemned);
    columns[i++]->insert(entries_graduated);
    columns[i++]->insert(entries_redeleted);
    columns[i++]->insert(fence_outs);
    columns[i++]->insert(anomalies);
    columns[i++]->insert(duration_ms);
    columns[i++]->insert(error);
    {
        Map map;
        map.reserve(profile_events.size());
        for (const auto & [k, v] : profile_events)
            map.push_back(Tuple{k, v});
        columns[i++]->insert(map);
    }
    columns[i++]->insert(round_id);
    columns[i++]->insert(phase);
    columns[i++]->insert(phase_duration_microseconds);
    {
        Map map;
        map.reserve(phase_metrics.size());
        for (const auto & [k, v] : phase_metrics)
            map.push_back(Tuple{k, v});
        columns[i++]->insert(map);
    }
}

}
