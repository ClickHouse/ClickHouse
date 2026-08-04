#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartPathParser.h>
#include <algorithm>
#include <array>
#include <string_view>
#include <utility>

namespace DB::Cas
{

/// Split a path into non-empty components, treating repeated or leading/trailing '/' characters as
/// separators. The parser deliberately does not normalize or otherwise interpret components: path
/// classification must be based on the names supplied by the disk layer.
static std::vector<std::string> splitNonEmpty(const std::string & path)
{
    std::vector<std::string> parts;
    std::string cur;
    for (char c : path)
    {
        if (c == '/')
        {
            if (!cur.empty())
                parts.push_back(std::move(cur));
            cur.clear();
        }
        else
        {
            cur.push_back(c);
        }
    }
    if (!cur.empty())
        parts.push_back(std::move(cur));
    return parts;
}

namespace
{

/// The split of a disk-relative path into non-empty components is the dominant allocation of every
/// path classifier, and the CA metadata read path runs SEVERAL of them on the SAME raw path per
/// logical file-open (each of `existsFile` / `getFileSize` / `getStorageObjects` first calls
/// `isPartFilePath`, then `parsePartFilePath`). The split is a PURE function of the path, so a small
/// thread-local FIFO ring cache keyed on the raw path is always correct, disk-agnostic and
/// lock-free. It is a fixed-capacity round-robin ring, NOT an LRU/MRU: a hit does not move or
/// promote its slot, so under sustained eviction pressure a path can be re-split on a later call
/// even if it was seen recently — always still CORRECT (re-splitting just re-derives the same
/// result), only less effective as a cache. The returned reference stays valid until the next
/// `splitCached` call on the SAME thread; every classifier consumes its split before splitting
/// again (none splits while holding another's split).
struct SplitCache
{
    static constexpr size_t kCapacity = 8;
    std::array<std::pair<std::string, std::vector<std::string>>, kCapacity> slots;
    size_t count = 0;  /// populated slots (<= kCapacity)
    size_t next = 0;   /// round-robin insertion cursor
    size_t misses = 0; /// underlying `splitNonEmpty` invocations (observability / test oracle)

    /// Return the cached split for `path`, or replace the next round-robin slot with a newly split
    /// value. A hit does not promote its slot, so this is a fixed-capacity FIFO-style ring rather
    /// than an LRU. The returned reference remains valid until the next `get` call on this thread.
    const std::vector<std::string> & get(const std::string & path)
    {
        for (size_t i = 0; i < count; ++i)
            if (slots[i].first == path)
                return slots[i].second;
        ++misses;
        auto & slot = slots[next];
        slot.first = path;
        slot.second = splitNonEmpty(path);
        next = (next + 1) % kCapacity;
        if (count < kCapacity)
            ++count;
        return slot.second;
    }
};

thread_local SplitCache tls_split_cache;

const std::vector<std::string> & splitCached(const std::string & path)
{
    // Every caller consumes the returned components before asking for another split on this
    // thread, so the cache may safely reuse its ring slots between classifier invocations.
    return tls_split_cache.get(path);
}

}

/// Whether every character of `s` is a lowercase hex digit.
static bool isLowerHex(std::string_view s)
{
    return std::all_of(s.begin(), s.end(), [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}

/// Whether `s` has the exact shape of a canonical UUID string: 36 characters, dashes at positions
/// 8/13/18/23, lowercase hex everywhere else.
static bool looksLikeUuidDirName(std::string_view s)
{
    if (s.size() != 36)
        return false;
    for (size_t i = 0; i < 36; ++i)
    {
        const bool dash_pos = (i == 8 || i == 13 || i == 18 || i == 23);
        if (dash_pos != (s[i] == '-'))
            return false;
        if (!dash_pos && !((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f')))
            return false;
    }
    return true;
}

/// Locate the `<uuid[:3]>/<uuid>` anchor inside a split Atomic path. The leading prefix is normally
/// `store`, but may be absent from a disk-relative path, so the parser identifies the pair by its
/// shape instead of requiring a particular prefix. Return the index of the UUID component; the
/// component immediately after it is the part directory when one exists.
static std::optional<size_t> findTableUuidComponent(const std::vector<std::string> & p)
{
    for (size_t i = 1; i < p.size(); ++i)
    {
        const auto & prefix = p[i - 1];
        const auto & uuid = p[i];
        /// Shape-based on purpose (robust to a missing `store/`), but the shape is now the REAL
        /// Atomic one: 3 lowercase-hex chars followed by a well-formed UUID sharing that prefix —
        /// a 3-char database named like its table (`data/abc/abcxyz/...`) no longer false-anchors.
        if (prefix.size() == 3 && isLowerHex(prefix) && looksLikeUuidDirName(uuid)
            && uuid.compare(0, 3, prefix) == 0)
            return i;
    }
    return std::nullopt;
}

/// Return whether a component has the MergeTree part-directory grammar. Non-Atomic layouts do not
/// have an Atomic UUID anchor, so their part boundary is found from the final three non-empty
/// decimal underscore-separated groups: `_min_max_level`, optionally preceded by a mutation number.
/// The grammar also covers temporary and operation prefixes such as `tmp_insert_all_1_1_0` and
/// `delete_tmp_all_1_1_0`; this helper has no Storage dependency and is used only as the non-Atomic
/// fallback.
static bool looksLikePartDir(const std::string & name)
{
    std::vector<std::string> groups;
    std::string cur;
    for (char c : name)
    {
        if (c == '_')
        {
            groups.push_back(cur);
            cur.clear();
        }
        else
            cur.push_back(c);
    }
    groups.push_back(cur);

    // Need at least <partition>_<min>_<max>_<level>: a partition group plus 3 trailing numeric groups.
    if (groups.size() < 4)
        return false;

    auto is_number = [](const std::string & s)
    {
        if (s.empty())
            return false;
        for (char c : s)
            if (c < '0' || c > '9')
                return false;
        return true;
    };

    const size_t n = groups.size();
    return is_number(groups[n - 1]) && is_number(groups[n - 2]) && is_number(groups[n - 3]);
}

/// Describes the boundary found by `findPartDirComponent`: components in [table_start, part_idx)
/// form the table identifier, `part_idx` names the part or reserved part container, and all later
/// components form the in-part file path. Atomic identifiers contain one UUID component; non-Atomic
/// identifiers contain the complete `data/<db>/<tbl>` prefix.
struct PartDirAnchor
{
    size_t table_start;
    size_t part_idx;
};

/// Locate the part-directory component. Prefer the UUID anchor for Atomic paths. Without it, treat
/// the first reserved `detached` or `moving` component after the table root as the boundary, then
/// fall back to the rightmost component matching the part-directory grammar. The reserved-directory
/// scan must precede the right-to-left grammar scan: otherwise the inner part name would be mistaken
/// for the boundary and the reserved directory would become part of a spurious table namespace that
/// table cleanup does not own. Return nullopt when the path has no part component.
static std::optional<PartDirAnchor> findPartDirComponent(const std::vector<std::string> & p)
{
    if (auto uuid_idx = findTableUuidComponent(p))
    {
        const size_t part_idx = *uuid_idx + 1;
        if (part_idx < p.size())
        {
            // The reserved deduplication-log directory is a table-level subdir, not a part dir
            // (see kDeduplicationLogsDirName).
            if (p[part_idx] == kDeduplicationLogsDirName)
                return std::nullopt;
            return PartDirAnchor{*uuid_idx, part_idx}; // table id = the single <uuid> component
        }
        return std::nullopt; // table dir, no part component after the uuid
    }

    // No uuid anchor: a non-Atomic table path. `detached` (data/<db>/<table>/detached/<part>/...)
    // and `moving` (data/<db>/<table>/moving/<part>/...) are both reserved table-level subdirs,
    // exactly like the Atomic layout where the uuid anchor makes them the part_name for free.
    // Anchor on either FIRST (leftmost, index >= 1): the right-to-left part-dir scan below would
    // otherwise anchor on the INNER <part>-shaped component and fold the reserved dir into a
    // spurious table id (data/<db>/<table>/detached or .../moving) that DROP TABLE never cleans,
    // orphaning a permanently-live ref. Mirrors
    // route()'s part_name == kDetachedDirName / kMovingDirName folding.
    for (size_t i = 1; i < p.size(); ++i)
        if (p[i] == kDetachedDirName || p[i] == kMovingDirName)
            return PartDirAnchor{0, i}; // table id = the whole path before the reserved dir

    // A non-Atomic database or table literally named `detached` is necessarily interpreted as the
    // reserved directory. The two shapes are indistinguishable from a path string alone; resolving
    // the ambiguity requires caller-supplied knowledge of existing databases and tables, which this
    // pure string parser intentionally does not have. The reserved interpretation is retained so
    // ordinary detached-part paths continue to map into the real table namespace. The test
    // `CasPartPathParser.DetachedNamedTableIsKnownAmbiguityFoldedAsReservedDir` pins this behavior so
    // any future change here is a conscious one.

    // The table identifier must be at least one component (a real table dir, never the bare disk
    // root), so the part dir is at index >= 1. Scan right to left so a part-dir-shaped
    // table/partition name earlier in the path cannot steal the anchor.
    for (size_t i = p.size(); i-- > 1;)
        if (looksLikePartDir(p[i]))
            return PartDirAnchor{0, i}; // table id = the whole path before the part dir
    return std::nullopt;
}

/// Join components [start, end) with '/' into the stable table identifier used by the routing layer:
/// one UUID component for Atomic paths or the complete `data/<db>/<tbl>` path for non-Atomic paths.
static std::string joinTableId(const std::vector<std::string> & p, size_t start, size_t end)
{
    std::string id;
    for (size_t i = start; i < end; ++i)
    {
        if (!id.empty())
            id += "/";
        id += p[i];
    }
    return id;
}

/// Return the number of underlying `splitNonEmpty` invocations on the current thread for cache
/// observability and tests.
size_t splitCacheMissesForTest()
{
    return tls_split_cache.misses;
}

void resetSplitCacheForTest()
{
    tls_split_cache = SplitCache{};
}

std::optional<PartFilePath> parsePartFilePath(const std::string & path)
{
    const auto & p = splitCached(path);
    auto anchor = findPartDirComponent(p);
    if (!anchor)
        return std::nullopt;

    PartFilePath r;
    r.table_uuid = joinTableId(p, anchor->table_start, anchor->part_idx);
    r.part_name = p[anchor->part_idx];
    if (anchor->part_idx + 1 < p.size())
    {
        std::string file = p[anchor->part_idx + 1];
        for (size_t i = anchor->part_idx + 2; i < p.size(); ++i)
            file += "/" + p[i];
        r.file = file;
    }
    // FREEZE target: shadow/<backup_name>/.../<part>. Capture both the backup name (the component
    // right after the reserved "shadow" root) and the literal shadow table dir — the joined
    // components before the part dir — for the commit / read / remove routing. The inner uuid
    // anchor above is unaffected by the prefix.
    if (p.size() >= 2 && p[0] == kShadowDirName)
    {
        r.backup_name = p[1];
        r.shadow_table_dir = joinTableId(p, 0, anchor->part_idx);
    }
    return r;
}

std::optional<std::string> parseTableUuid(const std::string & path)
{
    const auto & p = splitCached(path);

    // Atomic layout: exactly the table dir <prefix...>/<uuid[:3]>/<uuid>[/] — nothing after the uuid.
    if (auto uuid_idx = findTableUuidComponent(p); uuid_idx && *uuid_idx + 1 == p.size())
        return p[*uuid_idx];

    // Non-Atomic layout: a directory path with no part-dir component is the table dir
    // data/<db>/<table>. Require at least two components so the bare disk root (or a single generic
    // dir) is never taken as a table dir.
    if (findTableUuidComponent(p))
        return std::nullopt; // had a uuid anchor but something followed it: not a table dir
    if (p.size() >= 2 && !findPartDirComponent(p))
        return joinTableId(p, 0, p.size());
    return std::nullopt;
}

bool isAtomicShardDir(const std::string & path)
{
    // The Atomic on-disk layout shards table dirs as `store/<u3>/<uuid>@cas@`, so `store/<u3>` is a
    // pure intermediate shard directory: the literal `store` root followed by exactly one 3-char
    // uuid-prefix component, with nothing after it. This is ambiguous with the non-Atomic
    // data/<db> fallback (both are two non-part components with no uuid anchor), so the router uses
    // this strict predicate to disambiguate before parseTableUuid.
    const auto & p = splitCached(path);
    return p.size() == 2 && p[0] == "store" && p[1].size() == 3;
}

bool endsWithTableUuidPair(const std::string & path)
{
    const auto & p = splitCached(path);
    auto uuid_idx = findTableUuidComponent(p);
    return uuid_idx && *uuid_idx + 1 == p.size();
}

bool isPartFilePath(const std::string & path)
{
    // A file inside a part dir: <table_path...>/<part>/<file> => at least one component after the
    // part dir, for both the Atomic and non-Atomic layouts.
    const auto & p = splitCached(path);
    auto anchor = findPartDirComponent(p);
    return anchor && anchor->part_idx + 1 < p.size();
}

std::optional<TableFilePath> parseTableFilePath(const std::string & path)
{
    const auto & p = splitCached(path);

    // Atomic layout: a table-level file lives under the table dir, i.e. at least one component
    // after the uuid. The tail is EVERYTHING after the uuid joined by '/', so a table-level file in
    // a subdirectory (deduplication_logs/deduplication_log_1.txt) keeps its full sub-path. A part
    // file is excluded earlier by isPartFilePath; this function is only reached for non-part paths.
    if (auto uuid_idx = findTableUuidComponent(p))
    {
        if (*uuid_idx + 1 >= p.size())
            return std::nullopt; // the bare table dir, no file tail
        TableFilePath r;
        r.table_uuid = p[*uuid_idx];
        r.tail = joinTableId(p, *uuid_idx + 1, p.size());
        return r;
    }

    // Non-Atomic layout: a path with no part-dir component whose last component is the table-level
    // file, and the components before it are the table dir data/<db>/<table>. Require the table id
    // to be at least one component (a real table, never the bare disk root).
    if (p.size() < 2 || findPartDirComponent(p))
        return std::nullopt;

    // A reserved table-level subdirectory (deduplication_logs/) splits the path explicitly: the
    // table id is everything before it, the tail is the reserved dir and everything under it.
    // Without this the generic "last component is the file" rule would fold the subdir into the
    // table id and mis-scope the log objects. Index >= 1 so the table id is never the bare root.
    for (size_t i = 1; i + 1 < p.size(); ++i)
    {
        if (p[i] == kDeduplicationLogsDirName)
        {
            TableFilePath r;
            r.table_uuid = joinTableId(p, 0, i);
            r.tail = joinTableId(p, i, p.size());
            return r;
        }
    }

    TableFilePath r;
    r.table_uuid = joinTableId(p, 0, p.size() - 1);
    r.tail = p.back();
    return r;
}

std::string mirroredArchiveNamespace(const std::string & table_uuid)
{
    if (table_uuid.find('/') == std::string::npos)
    {
        /// Atomic: a bare uuid; mirror ClickHouse's store/<u3>/<uuid> fanout.
        const std::string u3 = table_uuid.substr(0, 3);
        return "store/" + u3 + "/" + table_uuid + std::string(kCasArchiveSuffix);
    }
    /// Non-Atomic: a full data/<db>/<tbl> path already; append the suffix to the last segment.
    return table_uuid + std::string(kCasArchiveSuffix);
}

bool isShadowPath(const std::string & path)
{
    size_t i = 0;
    while (i < path.size() && path[i] == '/')
        ++i;
    const auto first_end = path.find('/', i);
    const std::string_view first = first_end == std::string::npos
        ? std::string_view(path).substr(i)
        : std::string_view(path).substr(i, first_end - i);
    return first == kShadowDirName;
}

}
