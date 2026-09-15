#include <Storages/System/StorageSystemContentAddressedMounts.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_STATE;
}

StorageSystemContentAddressedMounts::StorageSystemContentAddressedMounts(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"disk", std::make_shared<DataTypeString>(), "Name of the content-addressed disk."},
        {"server_root_id", std::make_shared<DataTypeString>(), "Server root id owning the mount slot."},
        {"server_uuid", std::make_shared<DataTypeUUID>(), "UUID of the server incarnation holding the lease."},
        {"hostname", std::make_shared<DataTypeString>(), "Hostname recorded in the lease body."},
        {"process_id", std::make_shared<DataTypeUInt64>(), "Process id recorded in the lease body."},
        {"writer_epoch", std::make_shared<DataTypeUInt64>(), "Fenced writer epoch of the incarnation."},
        {"renewal_sequence", std::make_shared<DataTypeUInt64>(), "Lease renewal sequence number."},
        {"started_at", std::make_shared<DataTypeDateTime64>(3), "Time when the lease started."},
        {"expires_at", std::make_shared<DataTypeDateTime64>(3), "Time when the lease expires."},
        {"min_active_build_sequence", std::make_shared<DataTypeUInt64>(), "Oldest in-flight build sequence (UINT64_MAX means the mount said farewell)."},
        {"gc_fenced", std::make_shared<DataTypeUInt8>(), "1 if GC fenced this slot out (terminal)."},
        {"state", std::make_shared<DataTypeString>(), "Mount slot state: live, expired, terminated, fenced or corrupt."},
        {"is_leader", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "1 if this server's GC scheduler holds this disk's leadership lease. NULL on rows describing other servers' mounts."},
        {"pending_reclaim", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()), "Cumulative condemned-minus-deleted backlog observed by this process's GC on this disk. NULL on rows describing other servers' mounts."},
        {"last_success_age_seconds", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Seconds since this disk's GC last led a round (0 if it never led). NULL on rows describing other servers' mounts."},
        {"wedged_namespace_count", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Ref-append lanes currently wedged on this disk. NULL on rows describing other servers' mounts."},
        {"lifecycle", std::make_shared<DataTypeString>(), "This server's content-addressed pool lifecycle for the disk (non-gated snapshot, always populated so a not-live disk stays visible): live, not_live, identity_lost, vanished, constructing (never started) or shutdown (torn down)."},
        {"lifecycle_reason", std::make_shared<DataTypeString>(), "The enum-clean sub-state word for a vanished disk: replaced or forgotten. Empty for every other lifecycle (so lifecycle || '(' || lifecycle_reason || ')' reads e.g. vanished(forgotten))."},
        {"lifecycle_detail", std::make_shared<DataTypeString>(), "The full typed reason text naming the actual cause when not live: the vanish diagnosis (data root replaced by a foreign pool / decommissioned by SYSTEM CAS FORGET at <time>) or the identity-loss message. Empty when live."},
        {"lifecycle_since", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "When this server entered the current non-live lifecycle state. NULL when live (or the state is not backed by a live pool)."},
    }));
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemContentAddressedMounts::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

Pipe StorageSystemContentAddressedMounts::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    MutableColumnPtr col_disk = ColumnString::create();
    MutableColumnPtr col_srid = ColumnString::create();
    MutableColumnPtr col_uuid = ColumnUUID::create();
    MutableColumnPtr col_host = ColumnString::create();
    MutableColumnPtr col_pid = ColumnUInt64::create();
    MutableColumnPtr col_epoch = ColumnUInt64::create();
    MutableColumnPtr col_seq = ColumnUInt64::create();
    MutableColumnPtr col_started = ColumnDateTime64::create(0, 3);
    MutableColumnPtr col_expires = ColumnDateTime64::create(0, 3);
    MutableColumnPtr col_min_active = ColumnUInt64::create();
    MutableColumnPtr col_fenced = ColumnUInt8::create();
    MutableColumnPtr col_state = ColumnString::create();
    MutableColumnPtr col_is_leader = ColumnNullable::create(ColumnUInt8::create(), ColumnUInt8::create());
    MutableColumnPtr col_pending = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    MutableColumnPtr col_last_success = ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create());
    MutableColumnPtr col_wedged = ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create());
    MutableColumnPtr col_lifecycle = ColumnString::create();
    MutableColumnPtr col_lifecycle_reason = ColumnString::create();
    MutableColumnPtr col_lifecycle_detail = ColumnString::create();
    MutableColumnPtr col_lifecycle_since = ColumnNullable::create(ColumnDateTime::create(), ColumnUInt8::create());

    const uint64_t now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    /// The three lifecycle columns are the same for every row of a given disk (one snapshot per disk).
    /// A lambda keeps them in lockstep with the row-emitting arms below so no arm can forget one.
    const auto appendLifecycle = [&](const CasLifecycleSnapshot & snap)
    {
        col_lifecycle->insert(snap.lifecycle);
        col_lifecycle_reason->insert(snap.reason);
        col_lifecycle_detail->insert(snap.detail);
        if (snap.since != 0)
            col_lifecycle_since->insert(static_cast<UInt64>(snap.since));
        else
            col_lifecycle_since->insertDefault();   /// NULL while live / no backing pool
    };

    for (const auto & [disk_name, disk] : context->getDisksMap())
    {
        auto * ca = ContentAddressedMetadataStorage::tryFromDisk(disk);
        if (!ca)
            continue;

        /// The NON-GATED lifecycle snapshot (spec §7, Factory class): I/O-free, truthful in EVERY state.
        /// This is what makes a not-live / stopped / vanished / never-started disk VISIBLE — the
        /// store()/listMounts path below may refuse or return nothing, but the disk still gets a row
        /// carrying its lifecycle truth, instead of silently vanishing from the table (the old behavior,
        /// where a store() refusal or an empty vanished-pool listing dropped the very disk under investigation).
        const CasLifecycleSnapshot snap = ca->lifecycleSnapshot();

        bool emitted_row = false;

        /// store() -> poolAccess() throws INVALID_STATE both when the disk is NOT MOUNTED (never started /
        /// shut down / unmounted) AND when the pool is TERMINAL (Vanished/IdentityLost, via
        /// throwIfLifecycleTerminal) -- exactly the states that must still produce a visible row. Only a
        /// Live or TransientNotLive pool returns a live handle and reaches the mount-listing path below; a
        /// terminal or unmounted disk falls through to the synthesized snapshot row. Catch ONLY that typed
        /// refusal: any OTHER exception is a genuine fault, so let it escape rather than fold a real bug
        /// into a benign-looking synthesized row.
        Cas::PoolPtr store;
        try
        {
            store = ca->store();
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::INVALID_STATE)
                throw;
            store = nullptr;
        }

        if (store)
        {
            const uint64_t skew_margin_ms = static_cast<uint64_t>(store->poolConfig().mount_lease_ttl_ms.count()) / 2;
            std::vector<Cas::MountInfo> mounts;
            bool list_ok = true;
            try
            {
                /// Introspection reads on the open fence: a row describing this disk's mount slots must
                /// still be produced when the local mount fence has already run down -- that is exactly
                /// the state an operator opens this table to look at.
                Cas::CasOperation op = store->openRequests().admit();
                mounts = Cas::listMounts(op, store->layout(), now_ms, skew_margin_ms);
            }
            catch (...)
            {
                /// A transient backend error listing THIS disk's mounts must not blind the operator to the
                /// other disks' rows, nor drop this disk entirely: fall through to the snapshot-only row so
                /// the lifecycle is still reported (the whole reason the table is non-gated).
                tryLogCurrentException(getLogger("StorageSystemContentAddressedMounts"),
                                       "listing mounts for disk '" + disk_name + "'");
                list_ok = false;
            }

            if (list_ok)
            {
                const auto health = ca->gcHealth();
                const String & local_srid = store->poolConfig().server_root_id;
                for (const auto & m : mounts)
                {
                    col_disk->insert(disk_name);
                    col_srid->insert(m.srid);
                    assert_cast<ColumnUUID &>(*col_uuid).insertValue(UUID(m.lease.server_uuid));
                    col_host->insert(m.lease.hostname);
                    col_pid->insert(m.lease.pid);
                    col_epoch->insert(m.lease.writer_epoch);
                    col_seq->insert(m.lease.seq);
                    assert_cast<ColumnDateTime64 &>(*col_started).insertValue(static_cast<Decimal64>(m.lease.started_at_ms));
                    assert_cast<ColumnDateTime64 &>(*col_expires).insertValue(static_cast<Decimal64>(m.lease.expires_at_ms));
                    col_min_active->insert(m.lease.min_active_build_sequence);
                    col_fenced->insert(static_cast<UInt8>(m.lease.gc_fenced));
                    col_state->insert(m.state);

                    /// GC health is a process-local fact about THIS server's scheduler. Stamping it onto
                    /// peer rows misreads as "peer B is GC leader" during incidents — NULL there instead.
                    const bool is_local_row = (m.srid == local_srid);
                    if (is_local_row && health)
                    {
                        col_is_leader->insert(static_cast<UInt8>(health->is_leader));
                        col_pending->insert(health->pending_reclaim);
                        col_last_success->insert(health->last_success_age_seconds);
                        col_wedged->insert(health->wedged_namespace_count);
                    }
                    else
                    {
                        col_is_leader->insertDefault();
                        col_pending->insertDefault();
                        col_last_success->insertDefault();
                        col_wedged->insertDefault();
                    }

                    /// The lifecycle snapshot describes THIS server's pool condition for the disk; it is the
                    /// same for every mount slot listed, peer rows included.
                    appendLifecycle(snap);
                    emitted_row = true;
                }
            }
        }

        /// Ensure the disk is VISIBLE even when no mount row was emitted: not mounted, store() refused,
        /// listMounts failed, or a vanished (replaced/forgotten) pool with no slots left. Synthesize ONE row from the
        /// non-gated snapshot, with every live/lease and GC-health column defaulted (0) / NULL.
        if (!emitted_row)
        {
            col_disk->insert(disk_name);
            col_srid->insert(snap.server_root_id);
            col_uuid->insertDefault();
            col_host->insertDefault();
            col_pid->insertDefault();
            col_epoch->insertDefault();
            col_seq->insertDefault();
            col_started->insertDefault();
            col_expires->insertDefault();
            col_min_active->insertDefault();
            col_fenced->insertDefault();
            col_state->insertDefault();
            col_is_leader->insertDefault();
            col_pending->insertDefault();
            col_last_success->insertDefault();
            col_wedged->insertDefault();
            appendLifecycle(snap);
        }
    }

    Columns res_columns;
    res_columns.emplace_back(std::move(col_disk));
    res_columns.emplace_back(std::move(col_srid));
    res_columns.emplace_back(std::move(col_uuid));
    res_columns.emplace_back(std::move(col_host));
    res_columns.emplace_back(std::move(col_pid));
    res_columns.emplace_back(std::move(col_epoch));
    res_columns.emplace_back(std::move(col_seq));
    res_columns.emplace_back(std::move(col_started));
    res_columns.emplace_back(std::move(col_expires));
    res_columns.emplace_back(std::move(col_min_active));
    res_columns.emplace_back(std::move(col_fenced));
    res_columns.emplace_back(std::move(col_state));
    res_columns.emplace_back(std::move(col_is_leader));
    res_columns.emplace_back(std::move(col_pending));
    res_columns.emplace_back(std::move(col_last_success));
    res_columns.emplace_back(std::move(col_wedged));
    res_columns.emplace_back(std::move(col_lifecycle));
    res_columns.emplace_back(std::move(col_lifecycle_reason));
    res_columns.emplace_back(std::move(col_lifecycle_detail));
    res_columns.emplace_back(std::move(col_lifecycle_since));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(storage_snapshot->metadata->getSampleBlock()), std::move(chunk)));
}

}
