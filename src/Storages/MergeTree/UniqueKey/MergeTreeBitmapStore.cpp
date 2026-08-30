#include <Storages/MergeTree/UniqueKey/MergeTreeBitmapStore.h>

#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

#include <algorithm>

namespace ProfileEvents
{
    extern const Event UniqueKeyBitmapLoadMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char unique_key_settle_staged_bitmap_fail[];
}

namespace
{

/// Not `part.name`: a bitmap file name may not depend on the table's `format_version`, and the two
/// spellings differ for the pre-custom-partitioning one.
String partNameV1(const IMergeTreeDataPart & part)
{
    return part.info.getPartNameV1();
}

/// The only `const_cast` of a part storage in `src/`, and deliberate: a bitmap sidecar is written
/// beside a part whose rows are immutable, so every caller here holds a `DataPartPtr`. There is no
/// host sibling -- stock code that mutates part storage holds a `MergeTreeMutableDataPartPtr`, which
/// no bitmap path has and none should acquire just to write a sidecar.
IDataPartStorage & mutableStorage(const IMergeTreeDataPart & part)
{
    return const_cast<IDataPartStorage &>(part.getDataPartStorage());
}

}

MergeTreeBitmapStore::MergeTreeBitmapStore(const MergeTreeData & data_, DeleteBitmapCachePtr cache_)
    : log(getLogger("MergeTreeBitmapStore"))
    , cache(std::move(cache_))
    , data(data_)
{
}

DataPartPtr MergeTreeBitmapStore::findPart(const MergeTreePartInfo & info) const
{
    return data.getPartIfExists(
        info, {MergeTreeData::DataPartState::Active, MergeTreeData::DataPartState::Outdated});
}

/// ---- Reads ----

IBitmapStore::BitmapAndVersion
MergeTreeBitmapStore::readBitmap(const MergeTreePartInfo & part_info, CSN snapshot_csn) const
{
    /// We might need to consult the keeper for authoritative CSN later
    auto component_guard = Coordination::setCurrentComponent("MergeTreeBitmapStore::readBitmap");

    const auto version = versionAt(part_info, snapshot_csn);
    if (!version)
        return {std::make_shared<DeleteBitmap>(), 0};

    const auto part = findPart(part_info);
    if (!part)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Delete bitmap version {} of part {} is indexed, but the part is in neither Active nor "
            "Outdated", version->csn, part_info.getPartNameV1());

    auto load = [&]
    {
        return readVersion(*part, *version);
    };

    if (!cache)
        return {load(), version->csn};

    const auto key = DeleteBitmapCache::makeKey(part->getDeleteBitmapCacheIdentity(), version->csn);
    auto [ptr, _loaded] = cache->getOrSet(key, load);
    return {std::move(ptr), version->csn};
}

ConstDeleteBitmapPtr MergeTreeBitmapStore::readLatestBitmap(const MergeTreePartInfo & part) const
{
    /// `UNBOUNDED_CSN`, not a snapshot: the candidate set is already committed-only, so the max is the newest
    return readBitmap(part, UNBOUNDED_CSN).first;
}

/// Deliberately a directory listing and not the index: this is `system.parts` introspection, and
/// what it must show is the files physically present -- including the ones this part stages for
/// others, which are versions of those targets and appear nowhere in this part's index entry.
std::vector<DeleteBitmapFileOps::BitmapFile>
MergeTreeBitmapStore::listBitmaps(const MergeTreePartInfo & part_info) const
{
    auto component_guard = Coordination::setCurrentComponent("MergeTreeBitmapStore::listBitmaps");

    const auto part = findPart(part_info);
    if (!part)
        return {};

    auto files = DeleteBitmapFileOps::enumerateFiles(part->getDataPartStorage());
    DeleteBitmapFileOps::sortByVersion(files);
    return files;
}

/// ---- The index ----

MergeTreeBitmapStore::PartEntryPtr MergeTreeBitmapStore::getOrCreateEntry(const MergeTreePartInfo & part) const
{
    std::lock_guard lock(entries_mutex);
    auto & entry = entries[part];
    if (!entry)
        entry = std::make_shared<PartEntry>();
    return entry;
}

MergeTreeBitmapStore::PartEntryPtr MergeTreeBitmapStore::findEntry(const MergeTreePartInfo & part) const
{
    std::lock_guard lock(entries_mutex);
    const auto it = entries.find(part);
    return it == entries.end() ? nullptr : it->second;
}

void MergeTreeBitmapStore::dropPart(const IMergeTreeDataPart & part)
{
    PartEntryPtr dropped;
    {
        std::lock_guard lock(entries_mutex);
        if (const auto it = entries.find(part.info); it != entries.end())
        {
            dropped = it->second;
            entries.erase(it);
        }
    }

    std::vector<MergeTreePartInfo> owed;
    if (dropped)
    {
        std::lock_guard entry_lock(dropped->mutex);
        owed = dropped->staged_for;
    }

    if (!owed.empty())
    {
        /// A divergence: `grabOldParts` holds an owing part back rather than removing it. Forget
        /// rather than throw -- the part is already out of the set, and leaking the index entry on
        /// top of the lost kills helps nobody.
        forgetStagedBitmaps(part.info, owed);

        Strings targets;
        targets.reserve(owed.size());
        for (const auto & target : owed)
            targets.push_back(target.getPartNameV1());

        LOG_ERROR(log,
            "Part '{}' left the part set still owing staged delete bitmaps for {} target(s) ({}). "
            "Those targets have been unlinked from it and no longer apply the kills those bitmaps "
            "carried",
            partNameV1(part), targets.size(), fmt::join(targets, ", "));
    }

    if (cache)
        cache->removeEntriesForPart(part.getDeleteBitmapCacheIdentity());
}

std::vector<CSN> MergeTreeBitmapStore::loadPart(const MergeTreePartInfo & part, const IDataPartStorage & storage)
{
    const auto files = DeleteBitmapFileOps::enumerateFiles(storage);

    std::vector<CSN> added;
    std::vector<MergeTreePartInfo> staged_targets;
    {
        const auto entry = getOrCreateEntry(part);
        std::lock_guard lock(entry->mutex);
        for (const auto & file : files)
        {
            if (file.isStaged())
            {
                const auto target = MergeTreePartInfo::tryParsePartName(
                    file.staged_for_target, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
                if (target)
                    staged_targets.push_back(*target);
                else
                    LOG_ERROR(log, "Staged bitmap '{}' in part '{}' does not name a parseable part; "
                        "reads of that target will not find it", file.name, part.getPartNameV1());
            }
            else if (std::find(entry->settled.begin(), entry->settled.end(), file.version) == entry->settled.end())
            {
                entry->settled.push_back(file.version);
                added.push_back(file.version);
            }
        }
        std::sort(entry->settled.begin(), entry->settled.end());
    }

    /// Outside the entry lock: `registerStagedBitmaps` takes the targets' locks
    if (!staged_targets.empty())
        registerStagedBitmaps(part, staged_targets);

    return added;
}

std::vector<MergeTreeBitmapStore::Version>
MergeTreeBitmapStore::stagedVersions(const std::vector<MergeTreePartInfo> & owners) const
{
    std::vector<Version> versions;
    versions.reserve(owners.size());
    for (const auto & owner_info : owners)
    {
        const auto owner = findPart(owner_info);
        if (!owner)
        {
            /// An owner enters the index only once `loadPart` has seen it, and one that owes a
            /// staged bitmap is held back from removal, so it cannot have left the part set.
            /// Skipping it would silently drop whatever kills the staged bitmap holds. `entries` is
            /// memory-only and rebuilt from disk by `loadPart`, so this cannot outlive the process
            /// that broke it.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Part {} is indexed as holding a staged delete bitmap but is in neither Active nor "
                "Outdated, so the kills in that bitmap cannot be resolved",
                owner_info.getPartNameV1());
        }

        /// TODO(unique-key): support REPEATABLE_READ, currently we ignore the COMMITTING
        const CSN csn = owner->version->getInfo().creation_csn;
        if (csn == Tx::UnknownCSN || csn == Tx::CommittingCSN || csn == Tx::RolledBackCSN)
            continue;

        versions.push_back({csn, owner});
    }
    return versions;
}

std::optional<MergeTreeBitmapStore::Version>
MergeTreeBitmapStore::versionAt(const MergeTreePartInfo & part, CSN snapshot_csn) const
{
    const auto entry = findEntry(part);
    if (!entry)
        return {};

    std::optional<Version> res;
    std::vector<MergeTreePartInfo> staged_owners;
    {
        std::lock_guard lock(entry->mutex);

        const auto & settled = entry->settled;
        const auto above = std::upper_bound(settled.begin(), settled.end(), snapshot_csn);
        if (above != settled.begin())
            res = Version{*std::prev(above), nullptr};

        staged_owners = entry->staged_owners;
    }

    /// Check if better choice in staged
    for (const auto & staged : stagedVersions(staged_owners))
        if (staged.csn <= snapshot_csn && (!res || staged.csn > res->csn))
            res = staged;

    return res;
}

DeleteBitmapPtr MergeTreeBitmapStore::readVersion(const IMergeTreeDataPart & part, const Version & version) const
{
    ProfileEventTimeIncrement<Time::Microseconds> measure(ProfileEvents::UniqueKeyBitmapLoadMicroseconds);
    const String name = partNameV1(part);

    if (version.staged_in)
        if (auto staged = DeleteBitmapFileOps::tryReadStagedFor(version.staged_in->getDataPartStorage(), name))
            return staged;

    if (auto settled = DeleteBitmapFileOps::tryReadVersion(part.getDataPartStorage(), version.csn))
        return settled;

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Delete bitmap version {} does not exist in part {}",
        version.csn, name);
}

/// ---- Stage ----

void MergeTreeBitmapStore::registerStagedBitmaps(
    const MergeTreePartInfo & owner, const std::vector<MergeTreePartInfo> & targets)
{
    for (const auto & target : targets)
    {
        const auto entry = getOrCreateEntry(target);
        std::lock_guard lock(entry->mutex);
        auto & owners = entry->staged_owners;
        if (std::find(owners.begin(), owners.end(), owner) == owners.end())
            owners.push_back(owner);
    }

    /// The reverse direction
    const auto entry = getOrCreateEntry(owner);
    std::lock_guard lock(entry->mutex);
    for (const auto & target : targets)
        if (std::find(entry->staged_for.begin(), entry->staged_for.end(), target) == entry->staged_for.end())
            entry->staged_for.push_back(target);
}

void MergeTreeBitmapStore::removeStagedFor(const MergeTreePartInfo & owner, const MergeTreePartInfo & target)
{
    if (const auto entry = findEntry(owner))
    {
        std::lock_guard lock(entry->mutex);
        std::erase(entry->staged_for, target);
    }
}

void MergeTreeBitmapStore::removeStagedOwner(const MergeTreePartInfo & owner, const MergeTreePartInfo & target)
{
    if (const auto entry = findEntry(target))
    {
        std::lock_guard lock(entry->mutex);
        std::erase(entry->staged_owners, owner);
    }
}

void MergeTreeBitmapStore::forgetStagedBitmaps(
    const MergeTreePartInfo & owner, const std::vector<MergeTreePartInfo> & targets)
{
    for (const auto & target : targets)
    {
        removeStagedOwner(owner, target);
        removeStagedFor(owner, target);
    }
}

/// ---- The settle ----

std::vector<MergeTreePartInfo> MergeTreeBitmapStore::stagedTargetsOf(const MergeTreePartInfo & owner) const
{
    const auto entry = findEntry(owner);
    if (!entry)
        return {};

    std::lock_guard lock(entry->mutex);
    return entry->staged_for;
}

IBitmapStore::SettleReport MergeTreeBitmapStore::settleStagedBitmaps(const MergeTreePartInfo & owner_info, CSN csn)
{
    return settleStagedBitmaps(owner_info, stagedTargetsOf(owner_info), csn);
}

IBitmapStore::SettleReport MergeTreeBitmapStore::settleStagedBitmaps(
    const MergeTreePartInfo & owner_info, const std::vector<MergeTreePartInfo> & targets, CSN csn)
{
    auto component_guard = Coordination::setCurrentComponent("MergeTreeBitmapStore::settleStagedBitmaps");

    const auto owner = findPart(owner_info);
    if (!owner)
    {
        /// An owing part is held back from removal, so this is a divergence. Not fail-closed: the
        /// staged files are still on disk, and the caller sees them counted as outstanding.
        LOG_ERROR(log,
            "Cannot settle the staged delete bitmaps of part {}: it is not in this table's part set, "
            "so nothing can be published out of it", owner_info.getPartNameV1());
        return {.failed = targets.size()};
    }

    SettleReport report;
    for (const auto & target : targets)
    {
        switch (settleOneStagedFile(*owner, target, csn))
        {
            case SettleOutcome::Published:
            case SettleOutcome::Unlinked:
                ++report.settled;
                break;
            case SettleOutcome::Deferred:
                ++report.deferred;
                break;
            case SettleOutcome::Failed:
                ++report.failed;
                break;
        }
    }

    return report;
}

MergeTreeBitmapStore::SettleOutcome MergeTreeBitmapStore::settleOneStagedFile(
    const IMergeTreeDataPart & owner,
    const MergeTreePartInfo & target_info,
    CSN csn)
{
    try
    {
        /// Resolved before any entry lock: `findPart` takes the table's own locks
        const auto target = findPart(target_info);
        if (!target)
            return sweepOrphanTarget(owner, target_info);

        /// Before the publish, so the staged copy is still the durable record when the caller sees the failure
        fiu_do_on(FailPoints::unique_key_settle_staged_bitmap_fail,
            { throw Exception(ErrorCodes::ABORTED,
                "Injected settle failure for the bitmap staged for '{}'", target_info.getPartNameV1()); });

        return publishStagedFile(owner, *target, csn);
    }
    catch (...)
    {
        tryLogCurrentException(log,
            fmt::format("Could not settle the delete bitmap staged in part '{}' for '{}'",
                partNameV1(owner), target_info.getPartNameV1()));
        return SettleOutcome::Failed;
    }
}

MergeTreeBitmapStore::SettleOutcome MergeTreeBitmapStore::sweepOrphanTarget(
    const IMergeTreeDataPart & owner, const MergeTreePartInfo & target)
{
    /// "Reclaimed" and "not loaded yet" are indistinguishable here, so only UNLINKING waits for the
    /// load -- including a readonly table, whose Outdated set is never scheduled and so never becomes
    /// complete. Deferring forever is correct there: nothing may be unlinked from such a table.
    if (!data.outdatedPartsSetIsComplete())
        return SettleOutcome::Deferred;

    removeStagedOwner(owner.info, target);

    DeleteBitmapFileOps::removeStagedFile(
        mutableStorage(owner), DeleteBitmap::fileNameForStagedTarget(target.getPartNameV1()));

    removeStagedFor(owner.info, target);

    LOG_TRACE(log, "Removed the orphan delete bitmap staged in part '{}' for '{}'",
        partNameV1(owner), target.getPartNameV1());
    return SettleOutcome::Unlinked;
}

MergeTreeBitmapStore::SettleOutcome MergeTreeBitmapStore::publishStagedFile(
    const IMergeTreeDataPart & owner,
    const IMergeTreeDataPart & target,
    CSN csn)
{
    DeleteBitmapFileOps::settleStagedFile(
        mutableStorage(owner), DeleteBitmap::fileNameForStagedTarget(partNameV1(target)),
        mutableStorage(target), csn, partNameV1(owner));

    {
        const auto entry = getOrCreateEntry(target.info);
        std::lock_guard lock(entry->mutex);

        /// In common case the insert is at the end, but in theory the publish can be out-of-order
        auto & settled = entry->settled;
        const auto pos = std::lower_bound(settled.begin(), settled.end(), csn);
        if (pos == settled.end() || *pos != csn)
            settled.insert(pos, csn);
        chassert(std::is_sorted(settled.begin(), settled.end()));
        std::erase(entry->staged_owners, owner.info);
    }

    /// Outside the target's entry lock
    removeStagedFor(owner.info, target.info);
    return SettleOutcome::Published;
}

/// ---- The gc ----

size_t MergeTreeBitmapStore::removeObsoleteBitmaps(const MergeTreePartInfo & part_info, CSN oldest_snapshot_csn)
{
    auto component_guard = Coordination::setCurrentComponent("MergeTreeBitmapStore::removeObsoleteBitmaps");

    const auto part = findPart(part_info);
    if (!part)
        return 0;

    const auto floor_version = versionAt(part->info, oldest_snapshot_csn);
    if (!floor_version)
        return 0;

    const auto entry = findEntry(part_info);
    if (!entry)
        return 0;

    std::vector<CSN> to_remove;
    {
        std::lock_guard lock(entry->mutex);
        auto & settled = entry->settled;
        /// Remove the prefix: [csn < floor_version->csn]
        const auto end = std::lower_bound(settled.begin(), settled.end(), floor_version->csn);
        to_remove.insert(to_remove.end(), settled.begin(), end);
        chassert(std::is_sorted(to_remove.begin(), to_remove.end()));
    }

    /// Outside the entry lock: the floor below is what keeps a live reader off these versions
    auto & storage = mutableStorage(*part);
    std::vector<CSN> removed;
    for (auto version : to_remove)
    {
        const bool existed = DeleteBitmapFileOps::removeVersion(storage, version);
        if (cache)
            cache->remove(DeleteBitmapCache::makeKey(part->getDeleteBitmapCacheIdentity(), version));
        if (!existed)
        {
            LOG_WARNING(log, "Try to remove obsolete delete bitmap version {} of part {} (oldest_snapshot={}), but the file does not exist",
                        version, part_info.getPartNameV1(), oldest_snapshot_csn);
            continue;
        }

        removed.push_back(version);
    }
    LOG_TRACE(log, "Removed obsolete delete bitmap versions {} of part {} (oldest_snapshot={})",
                fmt::join(removed, ","), part_info.getPartNameV1(), oldest_snapshot_csn);

    /// By value, not by re-running the bisect: a settle can publish a version below the floor
    /// It's fine for this infrequent cleanup
    {
        std::lock_guard lock(entry->mutex);
        std::erase_if(entry->settled, [&](CSN version)
        {
            return std::binary_search(to_remove.begin(), to_remove.end(), version);
        });
        chassert(std::is_sorted(entry->settled.begin(), entry->settled.end()));
    }
    return removed.size();
}

}
