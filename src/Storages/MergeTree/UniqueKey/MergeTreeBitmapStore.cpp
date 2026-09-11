#include <Storages/MergeTree/UniqueKey/MergeTreeBitmapStore.h>

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
    return data.getPartIfExists(info, RESOLVABLE_STATES);
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
            /// Held past the erase so the check below runs with `entries_mutex` released.
            dropped = it->second;
            entries.erase(it);
        }
    }

    std::vector<MergeTreePartInfo> owed;
    if (dropped)
    {
        std::vector<BitmapLink> held;
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

            /// A version is cumulative, so a target left with no holder means the newest one was
            /// already carried forward. Index-only, not a part lookup:
            /// `forcefullyMovePartToDetachedAndRemoveFromMemory` calls this under the parts lock.
            const auto target_entry = findEntry(link.target);
            if (!target_entry)
                continue;

            std::lock_guard target_lock(target_entry->mutex);
            if (target_entry->inward.empty())
                orphaned.push_back(link.target.getPartNameV1());
        }

        if (!orphaned.empty())
            LOG_ERROR(log,
                "Part '{}' left the part set holding the only delete bitmap of {} target(s) ({}), "
                "and its directory is already removed, so the kills it held are lost",
                partNameV1(part), orphaned.size(), fmt::join(orphaned, ", "));
    }

    if (cache)
        cache->removeEntriesForPart(part.getDeleteBitmapCacheIdentity());
}

void MergeTreeBitmapStore::loadPart(const MergeTreePartInfo & part, const IDataPartStorage & storage)
{
    std::vector<BitmapLink> links;
    for (const auto & file : DeleteBitmapFileOps::enumerateFiles(storage))
    {
        const auto target = MergeTreePartInfo::tryParsePartName(
            file.target, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        if (!target)
        {
            LOG_ERROR(log, "Delete bitmap '{}' in part '{}' does not name a parseable part, so "
                "reads of that target will not find it",
                file.fileName(), part.getPartNameV1());
            continue;
        }

        links.push_back({*target, file.version});
    }

    if (!links.empty())
        registerLinks(part, links);
}

std::vector<MergeTreeBitmapStore::HeldBy> MergeTreeBitmapStore::getInwardLinks(const MergeTreePartInfo & target) const
{
    const auto entry = findEntry(target);
    if (!entry)
        return {};

    /// Copied out, so a caller resolving these runs with no entry mutex held.
    std::lock_guard lock(entry->mutex);
    return entry->inward;
}

std::vector<MergeTreeBitmapStore::Version>
MergeTreeBitmapStore::heldVersions(const std::vector<HeldBy> & links) const
{
    std::vector<Version> versions;
    versions.reserve(links.size());
    for (const auto & link : links)
    {
        const auto holder = findPart(link.holder);
        if (!holder)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Part {} is indexed as holding a delete bitmap but is in neither Active nor "
                "Outdated, so the kills in that bitmap cannot be resolved",
                link.holder.getPartNameV1());
        }

        if (link.csn != 0)
            versions.push_back({link.csn, holder, /*carried=*/true});
        else if (const auto own = resolveOwnVersion(holder))
            versions.push_back(*own);
    }
    return versions;
}

std::optional<MergeTreeBitmapStore::Version>
MergeTreeBitmapStore::resolveOwnVersion(const DataPartPtr & holder)
{
    /// TODO(unique-key): support REPEATABLE_READ, currently we ignore the COMMITTING
    const CSN csn = holder->version->getInfo().creation_csn;
    if (!Tx::isCommittedCSN(csn))
        return {};
    return Version{csn, holder, /*carried=*/false};
}

std::optional<MergeTreeBitmapStore::Version>
MergeTreeBitmapStore::versionAt(const MergeTreePartInfo & part, CSN snapshot_csn) const
{
    std::optional<Version> res;
    for (const auto & held : heldVersions(getInwardLinks(part)))
        if (held.csn <= snapshot_csn && (!res || held.csn > res->csn))
            res = held;

    return res;
}

DeleteBitmapPtr MergeTreeBitmapStore::readVersion(const IMergeTreeDataPart & part, const Version & version) const
{
    ProfileEventTimeIncrement<Time::Microseconds> measure(ProfileEvents::UniqueKeyBitmapLoadMicroseconds);
    const String name = partNameV1(part);

    /// One name, no fallback. Nothing moves a bitmap after its part is published, so the index
    /// knows exactly where the bytes are -- and a resolver that tried the other names could not
    /// tell a wrong name from a missing file, which is how a lost bitmap reads as an empty one.
    chassert(version.held_in);
    const auto file = version.fileFor(name);
    if (auto held = DeleteBitmapFileOps::tryReadBitmap(version.held_in->getDataPartStorage(), file))
        return held;

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Delete bitmap version {} of part {} is indexed as held by part {}, but that part's "
        "directory does not have it",
        version.csn, name, partNameV1(*version.held_in));
}

/// ---- The index ----

void MergeTreeBitmapStore::registerLinks(const MergeTreePartInfo & holder, const std::vector<BitmapLink> & links)
{
    for (const auto & link : links)
    {
        const auto entry = getOrCreateEntry(link.target);
        std::lock_guard lock(entry->mutex);
        const HeldBy back{holder, link.csn};
        if (std::find(entry->inward.begin(), entry->inward.end(), back) == entry->inward.end())
            entry->inward.push_back(back);
    }

    /// The other end
    const auto entry = getOrCreateEntry(holder);
    std::lock_guard lock(entry->mutex);
    for (const auto & link : links)
        if (std::find(entry->outward.begin(), entry->outward.end(), link) == entry->outward.end())
            entry->outward.push_back(link);
}

void MergeTreeBitmapStore::registerStagedBitmaps(const MergeTreePartInfo & holder, const std::vector<MergeTreePartInfo> & targets)
{
    std::vector<BitmapLink> links;
    links.reserve(targets.size());
    for (const auto & target : targets)
        links.push_back({target, /*csn=*/0});
    registerLinks(holder, links);
}

void MergeTreeBitmapStore::removeLink(const MergeTreePartInfo & holder, const MergeTreePartInfo & target, CSN csn)
{
    if (const auto entry = findEntry(target))
    {
        std::lock_guard lock(entry->mutex);
        std::erase(entry->inward, HeldBy{holder, csn});
    }

    if (const auto entry = findEntry(holder))
    {
        std::lock_guard lock(entry->mutex);
        std::erase(entry->outward, BitmapLink{target, csn});
    }
}

void MergeTreeBitmapStore::removeAllLinks(const MergeTreePartInfo & holder, const MergeTreePartInfo & target)
{
    if (const auto entry = findEntry(target))
    {
        std::lock_guard lock(entry->mutex);
        std::erase_if(entry->inward, [&](const HeldBy & link) { return link.holder == holder; });
    }

    if (const auto entry = findEntry(holder))
    {
        std::lock_guard lock(entry->mutex);
        std::erase_if(entry->outward, [&](const BitmapLink & link) { return link.target == target; });
    }
}

void MergeTreeBitmapStore::removeStagedBitmaps(const MergeTreePartInfo & holder, const std::vector<MergeTreePartInfo> & targets)
{
    for (const auto & target : targets)
        removeAllLinks(holder, target);
}

/// ---- The carry ----

std::vector<IBitmapStore::CarriedBitmap>
MergeTreeBitmapStore::selectCarriedBitmaps(const std::vector<MergeTreePartInfo> & sources) const
{
    auto component_guard = Coordination::setCurrentComponent("MergeTreeBitmapStore::selectCarriedBitmaps");

    const std::unordered_set<MergeTreePartInfo> merged(sources.begin(), sources.end());

    /// Newest per target: a version is cumulative, so the greatest one the retiring sources hold
    /// covers every older one, and keeping one copy per target is what bounds the carry.
    std::map<MergeTreePartInfo, CarriedBitmap> newest;

    for (const auto & source_info : sources)
    {
        const auto source = findPart(source_info);
        if (!source)
            throw Exception(ErrorCodes::ABORTED,
                "Cannot merge part {}: it is in neither Active nor Outdated, so the delete bitmaps "
                "it holds for other parts cannot be read, and dropping them would resurrect rows",
                source_info.getPartNameV1());

        /// One per source, not one per link: it reads the source's own `creation_csn`.
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

            const auto version = link.csn != 0
                ? std::optional<Version>{{link.csn, source, /*carried=*/true}}
                : source_version;
            if (!version)
                continue;

            auto & slot = newest[link.target];
            if (slot.link.csn >= version->csn)
                continue;

            /// The name the bytes have in the SOURCE, which is the staged one when the source
            /// wrote them itself. What they will be called in the result is the carried form of
            /// `link`, and the copy is the caller's to perform.
            slot = {{link.target, version->csn}, source, version->fileFor(link.target.getPartNameV1())};
        }
    }

    std::vector<CarriedBitmap> result;
    result.reserve(newest.size());
    for (auto & [_, version] : newest)
        result.push_back(std::move(version));
    return result;
}

std::vector<IBitmapStore::BitmapLink> MergeTreeBitmapStore::getOutwardLinks(const MergeTreePartInfo & holder) const
{
    const auto entry = findEntry(holder);
    if (!entry)
        return {};

    std::lock_guard lock(entry->mutex);
    return entry->outward;
}

bool MergeTreeBitmapStore::hasPublishedInwardLink(
    const MergeTreePartInfo & target, const MergeTreePartInfo & holder, CSN csn, const DataPartsAnyLock & lock) const
{
    /// A part that covers `holder` is a merge result `holder` was a source of, and such a merge
    /// copies its sources' bitmaps for outside targets into the result. The version has to
    /// match: a snapshot between two versions still reads the older one, and letting a newer one
    /// release it is the GC floor's decision, not the pin's.
    for (const auto & other : getInwardLinks(target))
        if (other.csn == csn && other.holder != holder && other.holder.contains(holder))
        {
            const auto carrier = data.getPartIfExistsUnlocked(other.holder, RESOLVABLE_STATES, lock);
            if (carrier && Tx::isCommittedCSN(carrier->version->getInfo().creation_csn))
                return true;
        }

    return false;
}

bool MergeTreeBitmapStore::isPinned(const IMergeTreeDataPart & part, const DataPartsAnyLock & lock) const
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

    /// A version is cumulative, so the floor covers everything below it and nothing that survives
    /// can read one of those. Removal is the only thing a published part's file set is ever
    /// allowed to do, and this floor is what makes it unobservable.
    std::vector<HeldBy> holders;
    {
        std::lock_guard lock(entry->mutex);
        holders = entry->inward;
    }

    /// Classified out here: a link with no recorded csn takes its holder's, which needs the part,
    /// and resolving one takes the table's locks. The resolved csn is kept -- it is what the
    /// cache is keyed by, and only the link's `csn` says which name the file has.
    struct Obsolete
    {
        HeldBy link;
        CSN csn;
        /// The pin paid for by the lookup above, carried rather than taken again below.
        DataPartPtr holder;
    };
    std::vector<Obsolete> obsolete;
    for (const auto & link : holders)
    {
        const auto holder = findPart(link.holder);
        if (!holder)
            continue;

        CSN csn = link.csn;
        if (csn == 0)
        {
            const auto own = resolveOwnVersion(holder);
            if (!own)
                continue;
            csn = own->csn;
        }

        if (csn < floor_version->csn)
            obsolete.push_back({link, csn, holder});
    }

    /// The superseded bitmaps, in the parts that hold them. Unlinking one can leave its holder
    /// with nothing left to hold, which is what lets `grabOldParts` finally take a spent marker.
    size_t removed = 0;
    for (const auto & [link, csn, holder] : obsolete)
    {
        /// Index first: a reader that has already resolved this version holds the bytes, and one
        /// that has not must not be sent to a file about to go.
        removeLink(link.holder, part_info, link.csn);

        const DeleteBitmapFileOps::BitmapFile file{link.csn, part_info.getPartNameV1()};
        const bool existed = DeleteBitmapFileOps::removeBitmapFile(mutableStorage(*holder), file);
        if (cache)
            cache->remove(DeleteBitmapCache::makeKey(part->getDeleteBitmapCacheIdentity(), csn));
        if (!existed)
        {
            LOG_WARNING(log, "Try to remove obsolete delete bitmap version {} of part {} held by part {} "
                        "(oldest_snapshot={}), but the file does not exist",
                        csn, part_info.getPartNameV1(), link.holder.getPartNameV1(), oldest_snapshot_csn);
            continue;
        }
        ++removed;
    }

    LOG_TRACE(log, "Removed {} obsolete delete bitmap version(s) of part {} below csn {} (oldest_snapshot={})",
                removed, part_info.getPartNameV1(), floor_version->csn, oldest_snapshot_csn);

    return removed;
}

}
