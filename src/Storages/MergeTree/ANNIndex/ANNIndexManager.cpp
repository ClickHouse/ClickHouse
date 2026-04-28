#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNIndexManager.h>

#include <Storages/MergeTree/ANNIndex/ANNGroupStorageDiskFull.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <Core/UUID.h>
#include <Disks/DirectoryIterator.h>
#include <Disks/IDisk.h>
#include <Disks/IDiskTransaction.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

#include <IO/WriteHelpers.h>

#include <algorithm>
#include <filesystem>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace fs = std::filesystem;

namespace
{
    bool hasGroupDirPrefix(std::string_view name)
    {
        return name.starts_with(ANN_GROUP_ACTIVE_PREFIX)
            || name.starts_with(ANN_GROUP_TMP_PREFIX)
            || name.starts_with(ANN_GROUP_DELETING_PREFIX);
    }

    /// Given an `ann_<uuid>` directory name, return the corresponding `deleting_ann_<uuid>`
    /// name used for the retired directory. The UUID portion is preserved verbatim so that
    /// failure-mode debugging can follow a directory through its full lifecycle by UUID.
    std::string deriveDeletingDirName(const std::string & active_dir)
    {
        if (!std::string_view(active_dir).starts_with(ANN_GROUP_ACTIVE_PREFIX))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "ANNIndexManager: expected active-prefixed directory, got `{}`", active_dir);
        return std::string(ANN_GROUP_DELETING_PREFIX)
             + active_dir.substr(ANN_GROUP_ACTIVE_PREFIX.size());
    }

    LoggerPtr resolveLogger(const LoggerPtr & supplied)
    {
        return supplied ? supplied : getLogger("ANNIndexManager");
    }
}

ANNIndexManager::ANNIndexManager(Config config_)
    : config(std::move(config_))
{
    if (!config.volume)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexManager requires a non-null volume");
    if (config.shape.dim == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexManager requires `shape.dim > 0`");

    config.log = resolveLogger(config.log);

    /// Always start with a non-null empty snapshot so that readers never have to special-case
    /// `nullptr` on the hot path.
    active.set(std::make_unique<const ANNActiveGroupsSnapshot>());
}

DiskPtr ANNIndexManager::getDisk() const
{
    return config.volume->getDisk(0);
}

template <typename F>
void ANNIndexManager::publishWithLock(F && func)
{
    std::lock_guard lk(write_mtx);

    auto current = active.get();
    std::vector<ANNIndexGroupPtr> next_groups = current ? current->groups : std::vector<ANNIndexGroupPtr>{};

    /// Caller is free to mutate both the next active list and the retired map.
    func(next_groups, retired_group_meta);

    auto next_snap = std::make_unique<ANNActiveGroupsSnapshot>();
    next_snap->groups = std::move(next_groups);
    active.set(std::unique_ptr<const ANNActiveGroupsSnapshot>(std::move(next_snap)));
}

void ANNIndexManager::registerGroup(ANNIndexGroupPtr new_group)
{
    if (!new_group)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexManager::registerGroup: null group");

    if (new_group->getShape() != config.shape)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ANNIndexManager::registerGroup: shape fingerprint does not match the table's");

    if (new_group->getHashSeed() != config.hash_seed)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ANNIndexManager::registerGroup: hash_seed does not match the table's");

    publishWithLock([&](std::vector<ANNIndexGroupPtr> & next_groups,
                         std::unordered_map<std::string, RetiredMeta> &)
    {
        next_groups.push_back(std::move(new_group));
    });
}

std::vector<std::string> ANNIndexManager::getRetiredGroupDirs() const
{
    std::lock_guard lk(write_mtx);
    std::vector<std::string> result;
    result.reserve(retired_group_meta.size());
    for (const auto & [name, _] : retired_group_meta)
        result.push_back(name);
    std::sort(result.begin(), result.end());
    return result;
}

void ANNIndexManager::invalidateAllGroupsForShapeChange()
{
    /// Rename every `ann_<uuid>` to `deleting_ann_<uuid>` and rewrite the table-level
    /// `meta.json` with the new shape. Both operations are routed through a single disk
    /// transaction so the in-memory snapshot and on-disk state can only move together.
    auto disk = getDisk();
    auto txn = disk->createTransaction();

    publishWithLock([&](std::vector<ANNIndexGroupPtr> & next_groups,
                         std::unordered_map<std::string, RetiredMeta> & retired)
    {
        const auto now = std::chrono::steady_clock::now();
        for (auto & grp : next_groups)
        {
            if (!grp)
                continue;

            const std::string active_dir = grp->getGroupDir();
            const std::string deleting_dir = deriveDeletingDirName(active_dir);

            const std::string rel_from = (fs::path(config.relative_root_path) / active_dir).string();
            const std::string rel_to   = (fs::path(config.relative_root_path) / deleting_dir).string();
            txn->moveDirectory(rel_from, rel_to);

            /// Refresh the still-live group handle so that in-flight searchers resolve file
            /// paths relative to the new directory name. The underlying file descriptors (held
            /// by the FFI searcher) are not affected by a plain directory rename on the
            /// supported filesystems.
            grp->rebindStorage(openGroupStorage(deleting_dir));
            retired[deleting_dir] = RetiredMeta{now, std::move(grp)};
        }
        next_groups.clear();

        ANNIndexTableMeta new_meta;
        new_meta.version = 1;
        new_meta.shape = config.shape;
        new_meta.hash_algo = config.hash_algo;
        new_meta.hash_seed = config.hash_seed;
        new_meta.writeTo(config.volume, config.relative_root_path, txn);
    });

    txn->commit();
}

void ANNIndexManager::invalidateGroupsForMutation(const std::vector<DataPartPtr> & affected_parts)
{
    if (affected_parts.empty())
        return;

    std::vector<AffectedRange> affected;
    affected.reserve(affected_parts.size());
    for (const auto & part : affected_parts)
    {
        if (!part)
            continue;
        affected.push_back(AffectedRange{
            hashPartitionId(part->info.getPartitionId()),
            static_cast<UInt64>(part->info.min_block),
            static_cast<UInt64>(part->info.max_block),
        });
    }

    invalidateGroupsForRanges(affected);
}

void ANNIndexManager::invalidateGroupsForRanges(const std::vector<AffectedRange> & affected)
{
    if (affected.empty())
        return;

    publishWithLock([&](std::vector<ANNIndexGroupPtr> & next_groups,
                         std::unordered_map<std::string, RetiredMeta> & retired)
    {
        const auto now = std::chrono::steady_clock::now();
        std::vector<ANNIndexGroupPtr> keep;
        keep.reserve(next_groups.size());

        for (auto & grp : next_groups)
        {
            if (!grp)
                continue;

            bool intersects = false;
            for (const auto & r : affected)
            {
                if (grp->containsPart(r.partition_hash, r.min_block, r.max_block))
                {
                    intersects = true;
                    break;
                }
            }

            if (intersects)
            {
                const std::string active_dir = grp->getGroupDir();
                const std::string deleting_dir = deriveDeletingDirName(active_dir);

                /// Plain `disk->moveDirectory` via the manager helper: shape is unchanged so
                /// `meta.json` stays put and no cross-entity transaction is needed.
                renameGroupDir(active_dir, deleting_dir);

                grp->rebindStorage(openGroupStorage(deleting_dir));
                retired[deleting_dir] = RetiredMeta{now, std::move(grp)};
            }
            else
            {
                keep.push_back(std::move(grp));
            }
        }

        next_groups = std::move(keep);
    });
}

bool ANNIndexManager::isPartCovered(const DataPartPtr & part) const
{
    if (!part)
        return false;

    const UInt64 partition_hash = hashPartitionId(part->info.getPartitionId());
    const UInt64 min_block = static_cast<UInt64>(part->info.min_block);
    const UInt64 max_block = static_cast<UInt64>(part->info.max_block);

    return isRangeCovered(partition_hash, min_block, max_block);
}

bool ANNIndexManager::isRangeCovered(UInt64 partition_hash, UInt64 min_block, UInt64 max_block) const
{
    auto snap = active.get();
    if (!snap || snap->empty())
        return false;

    for (const auto & grp : snap->groups)
    {
        if (grp && grp->containsPart(partition_hash, min_block, max_block))
            return true;
    }
    return false;
}

ANNBuildSelectedEntryPtr ANNIndexManager::selectPartsForBuild(
    const MergeTreeData & data,
    StorageSnapshotPtr storage_snapshot,
    ANNIndexDefinition definition,
    UInt64 min_rows,
    UInt64 max_rows,
    UInt64 max_parts) const
{
    /// Either cap set to zero is interpreted as "disabled" — defensive parity with the
    /// scheduler, and keeps the function a pure filter over current coverage + parts.
    if (max_rows == 0 || max_parts == 0)
        return nullptr;

    DataPartsVector parts = data.getDataPartsVectorForInternalUsage();

    std::vector<DataPartPtr> unindexed;
    unindexed.reserve(parts.size());
    for (auto & p : parts)
    {
        if (!p)
            continue;
        if (!isPartCovered(p))
            unindexed.push_back(std::move(p));
    }

    UInt64 pool_rows = 0;
    for (const auto & p : unindexed)
        pool_rows += p->rows_count;

    if (pool_rows < min_rows)
        return nullptr;

    /// Stable ordering — same partition together, parts inside a partition sorted by
    /// `min_block` ascending so the `.fbin` row order is deterministic.
    std::sort(unindexed.begin(), unindexed.end(),
        [](const DataPartPtr & a, const DataPartPtr & b)
        {
            if (a->info.getPartitionId() != b->info.getPartitionId())
                return a->info.getPartitionId() < b->info.getPartitionId();
            return a->info.min_block < b->info.min_block;
        });

    std::vector<DataPartPtr> selected;
    selected.reserve(std::min<size_t>(unindexed.size(), max_parts));
    UInt64 total = 0;
    for (auto & p : unindexed)
    {
        if (selected.size() >= max_parts)
            break;
        const UInt64 n = p->rows_count;
        /// Accept the first part unconditionally (a single oversized part is still indexable);
        /// subsequent parts are only added while the cumulative row count stays within cap.
        if (!selected.empty() && total + n > max_rows)
            break;
        total += n;
        selected.push_back(std::move(p));
    }

    if (selected.empty())
        return nullptr;

    auto entry = std::make_shared<ANNBuildSelectedEntry>();
    entry->selected_parts = std::move(selected);
    entry->storage_snapshot = std::move(storage_snapshot);
    entry->definition = std::move(definition);
    return entry;
}

UInt64 ANNIndexManager::hashPartitionId(const String & partition_id) const
{
    SipHash hasher(config.hash_seed, 0);
    hasher.update(partition_id.data(), partition_id.size());
    return hasher.get64();
}

std::vector<ANNSearchHit> ANNIndexManager::search(
    const float * query,
    size_t query_dim,
    size_t k,
    size_t rescoring_factor,
    const ANNSearchOverrides & overrides) const
{
    if (k == 0)
        return {};

    auto snap = active.get();
    if (!snap || snap->empty())
        return {};

    const size_t factor = std::max<size_t>(rescoring_factor, 1);
    const size_t target_k = k * factor;

    std::vector<ANNSearchHit> merged;
    merged.reserve(target_k * snap->groups.size());

    for (const auto & grp : snap->groups)
    {
        if (!grp)
            continue;

        const auto hits = grp->search(query, query_dim, target_k, overrides);
        for (const auto & h : hits)
            merged.push_back(ANNSearchHit{grp->lookup(h.internal_id), h.distance});
    }

    const size_t keep = std::min(target_k, merged.size());
    if (keep == 0)
        return {};

    std::partial_sort(
        merged.begin(),
        merged.begin() + keep,
        merged.end(),
        [](const ANNSearchHit & a, const ANNSearchHit & b) { return a.distance < b.distance; });

    merged.resize(keep);
    return merged;
}

ANNGroupStoragePtr ANNIndexManager::createGroupStorage(const std::string & tmp_dir) const
{
    const std::string rel = (fs::path(config.relative_root_path) / tmp_dir).string();

    /// Create the tmp directory eagerly so callers can begin writing immediately.
    auto disk = getDisk();
    if (!disk->existsDirectory(config.relative_root_path))
        disk->createDirectories(config.relative_root_path);
    disk->createDirectories(rel);

    return std::make_shared<ANNGroupStorageDiskFull>(config.volume, rel, /*shared_transaction=*/nullptr);
}

ANNGroupStoragePtr ANNIndexManager::openGroupStorage(const std::string & group_dir) const
{
    const std::string rel = (fs::path(config.relative_root_path) / group_dir).string();
    return std::make_shared<ANNGroupStorageDiskFull>(config.volume, rel, /*txn=*/nullptr);
}

void ANNIndexManager::renameGroupDir(const std::string & from_name, const std::string & to_name)
{
    const std::string rel_from = (fs::path(config.relative_root_path) / from_name).string();
    const std::string rel_to = (fs::path(config.relative_root_path) / to_name).string();
    auto disk = getDisk();
    disk->moveDirectory(rel_from, rel_to);
    LOG_DEBUG(config.log, "ANN group dir renamed `{}` -> `{}`", from_name, to_name);
}

std::vector<std::string> ANNIndexManager::listGroupDirsOnDisk() const
{
    std::vector<std::string> result;

    auto disk = getDisk();
    if (!disk->existsDirectory(config.relative_root_path))
        return result;

    for (auto it = disk->iterateDirectory(config.relative_root_path); it->isValid(); it->next())
    {
        const std::string name = it->name();
        if (!hasGroupDirPrefix(name))
            continue;
        const std::string child_rel = (fs::path(config.relative_root_path) / name).string();
        if (!disk->existsDirectory(child_rel))
            continue;
        result.push_back(name);
    }

    std::sort(result.begin(), result.end());
    return result;
}

void ANNIndexManager::loadFromDisk()
{
    std::lock_guard lk(write_mtx);

    auto disk = getDisk();
    if (!disk->existsDirectory(config.relative_root_path))
        disk->createDirectories(config.relative_root_path);

    /// A corrupt `meta.json` escapes from here: the caller (server startup) should abort
    /// rather than silently forget the table's index state.
    auto meta = ANNIndexTableMeta::loadOrEmpty(config.volume, config.relative_root_path);

    const bool meta_empty = (meta.shape == ANNIndexShapeFingerprint{});
    const bool shape_mismatch = !meta_empty && !(meta.shape == config.shape);

    const auto now = std::chrono::steady_clock::now();

    /// Phase 1: classify the on-disk directories and optionally perform the shape-mismatch
    /// rename. The rename only needs to touch the `ann_<uuid>` entries; `deleting_ann_<uuid>`
    /// and `tmp_ann_<uuid>` are already in a terminal state and are left alone.
    std::vector<std::string> active_dirs;
    std::vector<std::string> deleting_dirs;
    {
        for (auto it = disk->iterateDirectory(config.relative_root_path); it->isValid(); it->next())
        {
            const std::string name = it->name();
            const std::string child_rel = (fs::path(config.relative_root_path) / name).string();
            if (!disk->existsDirectory(child_rel))
                continue;

            if (std::string_view(name).starts_with(ANN_GROUP_ACTIVE_PREFIX))
                active_dirs.push_back(name);
            else if (std::string_view(name).starts_with(ANN_GROUP_DELETING_PREFIX))
                deleting_dirs.push_back(name);
            /// `tmp_ann_*` is deliberately ignored — left for the orphan-sweep pass.
        }
        std::sort(active_dirs.begin(), active_dirs.end());
        std::sort(deleting_dirs.begin(), deleting_dirs.end());
    }

    if (meta_empty)
    {
        /// First-ever load for this table. Write a `meta.json` that reflects the manager's
        /// configuration so that a later start can detect a shape change. No groups are
        /// retired — a fresh root should not contain anything anyway, but if a stray
        /// `ann_<uuid>` exists from a migration its fate will be decided by the usual load
        /// path below (shape must match).
        ANNIndexTableMeta new_meta;
        new_meta.version = 1;
        new_meta.shape = config.shape;
        new_meta.hash_algo = config.hash_algo;
        new_meta.hash_seed = config.hash_seed;
        new_meta.writeTo(config.volume, config.relative_root_path, /*txn=*/nullptr);
    }
    else if (shape_mismatch)
    {
        LOG_WARNING(config.log,
            "ANNIndexManager: meta.json shape does not match table shape; renaming {} active group(s) to retired",
            active_dirs.size());

        auto txn = disk->createTransaction();
        for (const auto & active_dir : active_dirs)
        {
            const std::string deleting_dir = deriveDeletingDirName(active_dir);
            const std::string rel_from = (fs::path(config.relative_root_path) / active_dir).string();
            const std::string rel_to   = (fs::path(config.relative_root_path) / deleting_dir).string();
            txn->moveDirectory(rel_from, rel_to);
            deleting_dirs.push_back(deleting_dir);
        }

        ANNIndexTableMeta new_meta;
        new_meta.version = 1;
        new_meta.shape = config.shape;
        new_meta.hash_algo = config.hash_algo;
        new_meta.hash_seed = config.hash_seed;
        new_meta.writeTo(config.volume, config.relative_root_path, txn);

        txn->commit();

        active_dirs.clear();
        std::sort(deleting_dirs.begin(), deleting_dirs.end());
    }

    /// Phase 2: materialise the active snapshot from the (possibly emptied) `active_dirs`
    /// and populate `retired_group_meta` from `deleting_dirs`.
    std::vector<ANNIndexGroupPtr> next_groups;

    auto retire_by_name = [&](const std::string & name)
    {
        retired_group_meta[name] = RetiredMeta{now, nullptr};
    };

    for (const auto & group_dir : active_dirs)
    {
        ANNIndexGroupPtr grp;
        try
        {
            auto storage = std::make_shared<ANNGroupStorageDiskFull>(
                config.volume,
                (fs::path(config.relative_root_path) / group_dir).string(),
                /*txn=*/nullptr);
            grp = ANNIndexGroup::load(std::move(storage), config.search_defaults);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(config.log,
                "ANNIndexManager: failed to load active group `{}`: {}; retiring",
                group_dir, e.displayText());
            retire_by_name(group_dir);
            continue;
        }
        catch (const std::exception & e)
        {
            LOG_ERROR(config.log,
                "ANNIndexManager: failed to load active group `{}`: {}; retiring",
                group_dir, e.what());
            retire_by_name(group_dir);
            continue;
        }

        if (!(grp->getShape() == config.shape))
        {
            LOG_ERROR(config.log,
                "ANNIndexManager: active group `{}` has shape mismatch with table; retiring",
                group_dir);
            retire_by_name(group_dir);
            continue;
        }

        if (grp->getHashSeed() != config.hash_seed)
        {
            LOG_ERROR(config.log,
                "ANNIndexManager: active group `{}` has hash_seed mismatch with table; retiring",
                group_dir);
            retire_by_name(group_dir);
            continue;
        }

        next_groups.push_back(std::move(grp));
    }

    /// Retired directories on disk are authoritative: each one must be tracked in the
    /// in-memory map so the GC path can pick it up after the grace window elapses. The group
    /// pointer is left as `nullptr` because we have no reason to open the FFI searcher for a
    /// directory that is already on its way out.
    for (const auto & d : deleting_dirs)
        retire_by_name(d);

    auto next_snap = std::make_unique<ANNActiveGroupsSnapshot>();
    next_snap->groups = std::move(next_groups);
    active.set(std::unique_ptr<const ANNActiveGroupsSnapshot>(std::move(next_snap)));
}

std::optional<ANNIndexManager::BuildReservation> ANNIndexManager::tryReserveBuildSlot()
{
    bool expected = false;
    if (!build_in_flight.compare_exchange_strong(
            expected, true,
            std::memory_order_acq_rel,
            std::memory_order_acquire))
    {
        return std::nullopt;
    }

    /// Generate the tmp directory name now (rather than later in `createGroupStorage`)
    /// so that the in-flight registration covers the entire window from slot acquire to
    /// either commit or rollback. The actual on-disk directory is only created when the
    /// caller invokes `createGroupStorage(reservation.tmpDir())`; cleanup tolerates that
    /// gap because `isPathKnown` is purely an in-memory check.
    const auto uuid = UUIDHelpers::generateV4();
    std::string tmp_dir = std::string(ANN_GROUP_TMP_PREFIX) + toString(uuid);

    {
        std::lock_guard lk(write_mtx);
        in_flight_builds.insert(tmp_dir);
    }

    return BuildReservation(this, std::move(tmp_dir));
}

void ANNIndexManager::rollbackBuildReservation(const std::string & tmp_dir) noexcept
{
    {
        std::lock_guard lk(write_mtx);
        in_flight_builds.erase(tmp_dir);
    }
    build_in_flight.store(false, std::memory_order_release);
}

bool ANNIndexManager::isPathKnown(const std::string & name) const
{
    /// Active groups: read the lock-free snapshot and compare each group's directory
    /// (relative to the manager root) by basename. The active path is always
    /// `<root>/ann_<uuid>/`, so we extract the leaf and compare to `name`.
    if (auto snap = active.get())
    {
        for (const auto & g : snap->groups)
        {
            if (!g)
                continue;
            /// `getGroupDir()` returns the leaf directory name (e.g. `ann_<uuid>`), which is
            /// exactly what `listGroupDirsOnDisk` enumerates. Virtual so the test fakes can
            /// provide a stable answer without owning a real storage.
            if (g->getGroupDir() == name)
                return true;
        }
    }

    std::lock_guard lk(write_mtx);
    if (retired_group_meta.contains(name))
        return true;
    if (in_flight_builds.contains(name))
        return true;
    return false;
}

ANNIndexManager::BuildReservation::BuildReservation(ANNIndexManager * manager_, std::string tmp_dir_)
    : manager(manager_)
    , tmp_dir(std::move(tmp_dir_))
{
}

ANNIndexManager::BuildReservation::BuildReservation(BuildReservation && other) noexcept
    : manager(other.manager)
    , tmp_dir(std::move(other.tmp_dir))
{
    other.manager = nullptr;
}

ANNIndexManager::BuildReservation & ANNIndexManager::BuildReservation::operator=(BuildReservation && other) noexcept
{
    if (this != &other)
    {
        if (manager)
            manager->rollbackBuildReservation(tmp_dir);
        manager = other.manager;
        tmp_dir = std::move(other.tmp_dir);
        other.manager = nullptr;
    }
    return *this;
}

ANNIndexManager::BuildReservation::~BuildReservation()
{
    if (manager)
        manager->rollbackBuildReservation(tmp_dir);
}

void ANNIndexManager::BuildReservation::commit() noexcept
{
    if (manager)
    {
        manager->rollbackBuildReservation(tmp_dir);
        manager = nullptr;
    }
}

std::chrono::steady_clock::time_point ANNIndexManager::getRetiredAt(const std::string & dir) const
{
    std::lock_guard lk(write_mtx);
    auto it = retired_group_meta.find(dir);
    if (it == retired_group_meta.end())
        return std::chrono::steady_clock::time_point{};
    return it->second.retired_at;
}

ANNIndexGroupPtr ANNIndexManager::getRetiredGroupPtr(const std::string & dir) const
{
    std::lock_guard lk(write_mtx);
    auto it = retired_group_meta.find(dir);
    if (it == retired_group_meta.end())
        return nullptr;
    return it->second.group_ptr;
}

void ANNIndexManager::forgetRetiredGroup(const std::string & dir)
{
    std::lock_guard lk(write_mtx);
    retired_group_meta.erase(dir);
}

}
#endif
