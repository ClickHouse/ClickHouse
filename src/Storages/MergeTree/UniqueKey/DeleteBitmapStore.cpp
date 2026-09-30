#include <Storages/MergeTree/UniqueKey/DeleteBitmapStore.h>

#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/Exception.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <map>
#include <unordered_set>

namespace ProfileEvents
{
    extern const Event UniqueKeyBitmapLoadMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The two states a part has to be in for anything it holds to be observable.
constexpr MergeTreeData::DataPartStates RESOLVABLE_STATES
    = {MergeTreeData::DataPartState::Active, MergeTreeData::DataPartState::Outdated};

/// Not `part.name`: a bitmap file name may not depend on the table's `format_version`, and the two
/// spellings differ for the pre-custom-partitioning one.
String partNameV1(const IMergeTreeDataPart & part)
{
    return part.info.getPartNameV1();
}

}

DeleteBitmapStore::DeleteBitmapStore(const MergeTreeData & data_, DeleteBitmapCachePtr cache_)
    : log(getLogger("DeleteBitmapStore"))
    , cache(std::move(cache_))
    , data(data_)
{
}

DataPartPtr DeleteBitmapStore::findPart(const MergeTreePartInfo & info, const DataPartsAnyLock * lock) const
{
    return lock ? data.getPartIfExistsUnlocked(info, RESOLVABLE_STATES, *lock)
                : data.getPartIfExists(info, RESOLVABLE_STATES);
}

/// ---- Reads ----

DeleteBitmapStore::BitmapAndVersion
DeleteBitmapStore::readBitmap(const MergeTreePartInfo & part_info, CSN snapshot_csn) const
{
    /// We might need to consult the keeper for authoritative CSN later
    auto component_guard = Coordination::setCurrentComponent("DeleteBitmapStore::readBitmap");

    const auto versions = versionsUpTo(part_info, snapshot_csn);
    if (versions.empty())
        return {std::make_shared<DeleteBitmap>(), 0};

    const auto part = findPart(part_info);
    if (!part)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Delete bitmap versions of part {} are indexed, but the part is in neither Active nor "
            "Outdated", part_info.getPartNameV1());

    /// The newest visible version names the union: a delta is never rewritten and the set below a
    /// csn only grows by appending, so one csn identifies one union for the life of the part.
    const CSN newest = versions.back().csn;

    auto load = [&]
    {
        auto united = std::make_shared<DeleteBitmap>();

        if (cache && versions.size() > 1)
        {
            const auto below = DeleteBitmapCache::makeKey(
                part->getDeleteBitmapCacheIdentity(),
                versions[versions.size() - 2].csn,
                versions.size() - 1);

            if (const auto prefix = cache->get(below))
            {
                united->merge(*prefix);
                united->merge(*readVersion(*part, versions.back()));
                return united;
            }
        }

        for (const auto & version : versions)
            united->merge(*readVersion(*part, version));

        return united;
    };

    if (!cache)
        return {load(), newest};

    const auto key = DeleteBitmapCache::makeKey(
        part->getDeleteBitmapCacheIdentity(), newest, versions.size());
    auto [ptr, _loaded] = cache->getOrSet(key, load);
    return {std::move(ptr), newest};
}

ConstDeleteBitmapPtr DeleteBitmapStore::readLatestBitmap(const MergeTreePartInfo & part) const
{
    /// `UNBOUNDED_CSN`, not a snapshot: the candidate set is already committed-only, so the max is the newest
    return readBitmap(part, UNBOUNDED_CSN).first;
}

/// Deliberately a directory listing and not the index: this is `system.parts` introspection, and
/// what it must show is the files physically present -- including the ones this part stages for
/// others, which are versions of those targets and appear nowhere in this part's index entry.
std::vector<DeleteBitmapFileOps::BitmapFile>
DeleteBitmapStore::listBitmaps(const MergeTreePartInfo & part_info) const
{
    auto component_guard = Coordination::setCurrentComponent("DeleteBitmapStore::listBitmaps");

    const auto part = findPart(part_info);
    if (!part)
        return {};

    auto files = DeleteBitmapFileOps::enumerateFiles(part->getDataPartStorage());
    DeleteBitmapFileOps::sortByVersion(files);
    return files;
}

/// ---- The index ----

DeleteBitmapStore::PartEntryPtr DeleteBitmapStore::getOrCreateEntry(const MergeTreePartInfo & part) const
{
    std::lock_guard lock(entries_mutex);
    auto & entry = entries[part];
    if (!entry)
        entry = std::make_shared<PartEntry>();
    return entry;
}

DeleteBitmapStore::PartEntryPtr DeleteBitmapStore::findEntry(const MergeTreePartInfo & part) const
{
    std::lock_guard lock(entries_mutex);
    const auto it = entries.find(part);
    return it == entries.end() ? nullptr : it->second;
}

void DeleteBitmapStore::dropPart(const IMergeTreeDataPart & part)
{
    PartEntryPtr dropped;
    {
        std::lock_guard lock(entries_mutex);
        if (const auto it = entries.find(part.info); it != entries.end())
        {
            /// Held past the erase so the check below runs with `entries_mutex` released.
            dropped = it->second;
            entries.erase(it);
        }
    }

    if (dropped)
    {
        OutwardLinks held;
        {
            std::lock_guard entry_lock(dropped->mutex);
            held = std::move(dropped->outward);
        }

        /// Outside the dropped entry's lock: the targets' locks come next. Unlinking is not
        /// optional -- a link to a part that is out of the set makes every later read of the
        /// target throw for as long as the process lives.
        Strings orphaned;
        for (const auto & link : held)
        {
            removeAllLinks(part.info, link.target);

            /// A merge's late kills against its own result: the entry just erased was theirs.
            if (link.target == part.info)
                continue;

            /// A version is a delta, so no holder is redundant: a target left with no holder has
            /// lost real kills. Index-only, not a part lookup:
            /// `forcefullyMovePartToDetachedAndRemoveFromMemory` calls this under the parts lock.
            const auto target_entry = findEntry(link.target);
            if (!target_entry)
                continue;

            std::lock_guard target_lock(target_entry->mutex);
            if (target_entry->inward.empty())
                orphaned.push_back(link.target.getPartNameV1());
        }

        std::sort(orphaned.begin(), orphaned.end());

        if (!orphaned.empty())
            LOG_ERROR(log,
                "Part '{}' left the part set holding the only delete bitmap of {} target(s) ({}), "
                "and its directory is already removed, so the kills it held are lost",
                partNameV1(part), orphaned.size(), fmt::join(orphaned, ", "));
    }

    if (cache)
        cache->removeEntriesForPart(part.getDeleteBitmapCacheIdentity());
}

void DeleteBitmapStore::loadPart(const MergeTreePartInfo & part, const IDataPartStorage & storage)
{
    std::vector<BitmapLink> links;
    for (const auto & file : DeleteBitmapFileOps::enumerateFiles(storage))
    {
        const auto target = MergeTreePartInfo::tryParsePartName(
            file.target, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        if (!target)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Delete bitmap '{}' in part '{}' does not name a parseable part, so the rows it "
                "kills cannot be found. Refusing to load the part rather than serve those rows",
                file.fileName(), part.getPartNameV1());

        links.push_back({*target, file.version});
    }

    if (!links.empty())
        registerLinks(part, links);
}

void DeleteBitmapStore::addInwardLink(PartEntry & entry, const HeldBy & link)
{
    const auto at = std::lower_bound(entry.inward.begin(), entry.inward.end(), link, ByCsn{});
    if (at == entry.inward.end() || !(*at == link))
        entry.inward.insert(at, link);
}

void DeleteBitmapStore::resolveUnknownVersions(
    PartEntry & entry, const DataPartsAnyLock * lock, OnMissingHolder on_missing) const
{
    std::vector<MergeTreePartInfo> holders;
    {
        std::lock_guard entry_lock(entry.mutex);
        for (auto it = std::lower_bound(entry.inward.begin(), entry.inward.end(), ByCsn::UNKNOWN_CSN_ORDER, ByCsn{});
             it != entry.inward.end(); ++it)
        {
            chassert(!it->carried, fmt::format("Carried link of holder {} has no version",
                it->holder.getPartNameV1()));
            holders.push_back(it->holder);
        }
    }

    for (const auto & holder_info : holders)
    {
        /// With no entry mutex held: this takes the parts lock, and `grabOldParts` reaches the
        /// same entry the other way round.
        const auto holder = findPart(holder_info, lock);
        if (!holder)
        {
            if (on_missing == OnMissingHolder::Throw)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Part {} is indexed as holding a delete bitmap but is in neither Active nor "
                    "Outdated, so the kills in that bitmap cannot be resolved",
                    holder_info.getPartNameV1());
            continue;
        }

        const auto own = resolveOwnVersion(holder);
        if (!own)
            continue;

        std::lock_guard entry_lock(entry.mutex);
        /// Found again: the tail can have moved, or another reader resolved this one already.
        const auto tail = std::lower_bound(entry.inward.begin(), entry.inward.end(), ByCsn::UNKNOWN_CSN_ORDER, ByCsn{});
        const auto it = std::find_if(tail, entry.inward.end(),
            [&](const HeldBy & link) { return link.holder == holder_info; });
        if (it == entry.inward.end())
            continue;

        entry.inward.erase(it);
        addInwardLink(entry, {holder_info, own->csn, /*carried=*/false});
    }
}

std::optional<DeleteBitmapStore::Version>
DeleteBitmapStore::resolveOwnVersion(const DataPartPtr & holder)
{
    /// TODO(unique-key): support REPEATABLE_READ, currently we ignore the COMMITTING
    const CSN csn = holder->version->getInfo().creation_csn;
    if (!isSettledCSN(csn))
        return {};
    return Version{csn, holder, /*carried=*/false};
}

std::vector<DeleteBitmapStore::Version>
DeleteBitmapStore::versionsUpTo(const MergeTreePartInfo & part, CSN snapshot_csn) const
{
    const auto entry = findEntry(part);
    if (!entry)
        return {};

    resolveUnknownVersions(*entry, /*lock=*/nullptr, OnMissingHolder::Throw);

    std::vector<HeldBy> visible;
    {
        std::lock_guard lock(entry->mutex);
        const auto above = std::upper_bound(entry->inward.begin(), entry->inward.end(), snapshot_csn, ByCsn{});
        visible.assign(entry->inward.begin(), above);
    }

    std::vector<Version> versions;
    for (size_t i = 0; i < visible.size();)
    {
        /// Repeated CSNs are grouped together; any holder of the same CSN will do.
        size_t end = i;
        while (end < visible.size() && visible[end].csn == visible[i].csn)
            ++end;

        DataPartPtr holder;
        const HeldBy * resolved = nullptr;
        for (size_t k = i; k < end && !holder; ++k)
        {
            holder = findPart(visible[k].holder);
            resolved = &visible[k];
        }

        if (!holder)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Every part indexed as holding version {} of the delete bitmap of part {} is in "
                "neither Active nor Outdated, so the kills in it cannot be resolved",
                visible[i].csn, part.getPartNameV1());

        versions.push_back(Version{resolved->csn, holder, resolved->carried});
        i = end;
    }

    return versions;
}

DeleteBitmapPtr DeleteBitmapStore::readVersion(const IMergeTreeDataPart & part, const Version & version) const
{
    ProfileEventTimeIncrement<Time::Microseconds> measure(ProfileEvents::UniqueKeyBitmapLoadMicroseconds);
    const String name = partNameV1(part);

    chassert(version.held_in);

    /// One name, no fallback. Nothing moves a bitmap after its part is published, so the index
    /// knows exactly where the bytes are -- and a resolver that tried the other names could not
    /// tell a wrong name from a missing file, which is how a lost bitmap reads as an empty one.
    const auto file = version.fileFor(name);
    if (auto held = DeleteBitmapFileOps::tryReadBitmap(version.held_in->getDataPartStorage(), file))
        return held;

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Delete bitmap version {} of part {} is indexed as held by part {}, but that part's "
        "directory does not have it",
        version.csn, name, partNameV1(*version.held_in));
}

/// ---- The index ----

void DeleteBitmapStore::registerLinks(const MergeTreePartInfo & holder, const std::vector<BitmapLink> & links)
{
    for (const auto & link : links)
    {
        const auto entry = getOrCreateEntry(link.target);
        std::lock_guard lock(entry->mutex);
        addInwardLink(*entry, {holder, link.csn, /*carried=*/link.csn != Tx::UnknownCSN});
    }

    /// The other end
    const auto entry = getOrCreateEntry(holder);
    std::lock_guard lock(entry->mutex);
    entry->outward.insert(links.begin(), links.end());
}

void DeleteBitmapStore::registerStagedBitmaps(const MergeTreePartInfo & holder, const std::vector<MergeTreePartInfo> & targets)
{
    std::vector<BitmapLink> links;
    links.reserve(targets.size());
    for (const auto & target : targets)
        links.push_back({target, /*csn=*/0});
    registerLinks(holder, links);
}

void DeleteBitmapStore::removeAllLinks(const MergeTreePartInfo & holder, const MergeTreePartInfo & target)
{
    if (const auto entry = findEntry(target))
    {
        /// By holder, which the csn order cannot answer. Same trade as the staged case above.
        std::lock_guard lock(entry->mutex);
        std::erase_if(entry->inward, [&](const HeldBy & link) { return link.holder == holder; });
    }

    if (const auto entry = findEntry(holder))
    {
        /// By target, which the link's own hash cannot answer. Rollback and `dropPart` only.
        std::lock_guard lock(entry->mutex);
        absl::erase_if(entry->outward, [&](const BitmapLink & link) { return link.target == target; });
    }
}

void DeleteBitmapStore::removeStagedBitmaps(const MergeTreePartInfo & holder, const std::vector<MergeTreePartInfo> & targets)
{
    for (const auto & target : targets)
        removeAllLinks(holder, target);
}

std::vector<DeleteBitmapStore::CarriedBitmap>
DeleteBitmapStore::selectCarriedBitmaps(const std::vector<MergeTreePartInfo> & sources) const
{
    auto component_guard = Coordination::setCurrentComponent("DeleteBitmapStore::selectCarriedBitmaps");

    const std::unordered_set<MergeTreePartInfo> merged(sources.begin(), sources.end());
    std::vector<CarriedBitmap> carried;

    for (const auto & source_info : sources)
    {
        const auto source = findPart(source_info);
        if (!source)
            throw Exception(ErrorCodes::ABORTED,
                "Cannot merge part {}: it is in neither Active nor Outdated, so the delete bitmaps "
                "it holds for other parts cannot be read, and dropping them would resurrect rows",
                source_info.getPartNameV1());

        const auto source_version = resolveOwnVersion(source);

        for (const auto & link : getOutwardLinks(source_info))
        {
            /// Absorbed, not carried: the merge rewrites the target's rows and simply does not
            /// write the dead ones, so the bitmap dies with its target.
            if (merged.contains(link.target))
                continue;

            /// A dead entry, derived rather than stored: the target has left the part set, so
            /// nothing can observe what this bitmap kills.
            const auto target = findPart(link.target);
            if (!target)
                continue;

            const auto version = link.csn != Tx::UnknownCSN
                ? std::optional<Version>{{link.csn, source, /*carried=*/true}}
                : source_version;
            if (!version)
                continue;

            carried.push_back(
                {{link.target, version->csn}, source, version->fileFor(link.target.getPartNameV1())});
        }
    }

    std::sort(carried.begin(), carried.end(), [](const CarriedBitmap & a, const CarriedBitmap & b)
    { return std::tie(a.link.target, a.link.csn) < std::tie(b.link.target, b.link.csn); });

    return carried;
}

std::vector<DeleteBitmapStore::BitmapLink> DeleteBitmapStore::getOutwardLinks(const MergeTreePartInfo & holder) const
{
    const auto entry = findEntry(holder);
    if (!entry)
        return {};

    std::lock_guard lock(entry->mutex);
    return {entry->outward.begin(), entry->outward.end()};
}

bool DeleteBitmapStore::hasPublishedInwardLink(
    const MergeTreePartInfo & target, const MergeTreePartInfo & holder, CSN csn, const DataPartsAnyLock & lock) const
{
    const auto entry = findEntry(target);
    if (!entry)
        return false;

    /// Tolerantly: throwing here would pin a part that could never be released.
    resolveUnknownVersions(*entry, &lock, OnMissingHolder::Skip);

    std::vector<HeldBy> carriers;
    {
        std::lock_guard entry_lock(entry->mutex);
        const auto [first, last] = std::equal_range(entry->inward.begin(), entry->inward.end(), csn, ByCsn{});
        /// A part that covers `holder` is a merge result `holder` was a source of, and such a
        /// merge copies its sources' bitmaps for outside targets into the result.
        for (auto it = first; it != last; ++it)
            if (it->holder != holder && it->holder.contains(holder))
                carriers.push_back(*it);
    }

    for (const auto & candidate : carriers)
    {
        const auto carrier = data.getPartIfExistsUnlocked(candidate.holder, RESOLVABLE_STATES, lock);
        if (carrier && isSettledCSN(carrier->version->getInfo().creation_csn))
            return true;
    }

    return false;
}

bool DeleteBitmapStore::isPinned(const IMergeTreeDataPart & part, const DataPartsAnyLock & lock) const
{
    const CSN holder_csn = part.version->getInfo().creation_csn;

    /// A rolled-back write's bitmaps never publish, so waiting for one would pin the part forever.
    if (holder_csn == Tx::RolledBackCSN)
        return false;

    for (const auto & link : getOutwardLinks(part.info))
    {
        /// A merge's own late kills die with it, so waiting on a carrier would wait forever.
        if (link.target == part.info)
            continue;

        const CSN csn = link.csn ? link.csn : holder_csn;
        if (hasPublishedInwardLink(link.target, part.info, csn, lock))
            continue;

        /// Still readable, and nowhere else: this part is the only place the kills exist.
        if (data.getPartIfExistsUnlocked(link.target, RESOLVABLE_STATES, lock))
            return true;
    }
    return false;
}

}
