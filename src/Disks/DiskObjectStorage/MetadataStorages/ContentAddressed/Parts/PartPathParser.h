#pragma once
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace DB::Cas
{

/// Classifies disk-relative ClickHouse paths for the content-addressed metadata-storage wiring.
/// It handles Atomic and non-Atomic table layouts, FREEZE shadow trees, detached and moving part
/// directories, table-level deduplication logs, projections, and temporary or operation part names.
/// The functions are pure and side-effect-free: they identify the table, part, and file portions,
/// while content-addressed key construction remains in `Cas::Layout`. The `Cas` core therefore
/// never needs to understand ClickHouse path conventions.

/// The literal first path component reserved for FREEZE snapshots
/// (shadow/<backup>/store/<uuid[:3]>/<uuid>/<part>/...).
inline constexpr std::string_view kShadowDirName = "shadow";

/// The MergeTree detached-parts directory. parsePartFilePath reports a detached path with
/// part_name == kDetachedDirName and the real detached part dir as the FIRST component of `file`.
/// The transaction and read routing re-split that `file` value to recover the detached part's
/// actual name and in-part path.
inline constexpr std::string_view kDetachedDirName = "detached";

/// The MergeTree part-mover staging directory (`MergeTreeData::MOVING_DIR_NAME`,
/// `MergeTreeData.h`). A part being relocated to another disk (explicit `ALTER … MOVE
/// PART|PARTITION`, or a background TTL/policy move) is cloned under TABLE/moving/<part>/
/// before the atomic rename into its final place. `parsePartFilePath` reports such a path with
/// part_name == kMovingDirName and the real part dir as the FIRST component of `file` -- the
/// exact same shape `kDetachedDirName` already produces, for free, on the Atomic layout (no parser
/// change is needed there: "moving" already lands on `part_idx`
/// because it is the component right after the table <uuid>, same as "detached"). Mirroring
/// detached, `route()` folds this onto a `moving/`-PREFIXED ref (kMovingRefPrefix) -- NOT the
/// part's final ref directly. Publishing the clone under the final ref would break move
/// crash-atomicity: a crash between the clone commit and the mover's rename would leave a
/// committed live ref before the swap ever happened, and `moving/`'s own startup cleanup
/// couldn't tell that premature ref apart from a real live part. The staging ref keeps the
/// pre-swap clone un-live; the mover's rename does a real ref repoint moving/<part> -> <part>.
inline constexpr std::string_view kMovingDirName = "moving";

/// Detached parts live inside the table's own archive namespace as refs keyed by this prefix —
/// `detached/PART` versus a live `PART`. One namespace per table; the live-vs-detached
/// name collision is impossible because the ref names differ. The routing prepends this to the
/// detached part name to form the ref, and the `TABLE/detached` container dir surfaces the
/// table's refs filtered to this prefix (stripped for display). No parallel detached namespace
/// exists anymore (the old `detachedNamespace` is gone).
inline constexpr std::string_view kDetachedRefPrefix = "detached/";

/// MOVE-to-CA fix: mirrors kDetachedRefPrefix exactly, but for the mover's `moving/` staging
/// dir instead of `detached/`. Keeps a moved-but-not-yet-swapped part's ref distinct from its
/// eventual live ref `<part>`, so the destination CA transaction (`clonePart`'s CA branch) can
/// publish it WITHOUT prematurely making it live or colliding with an existing live part of the
/// same name.
inline constexpr std::string_view kMovingRefPrefix = "moving/";

/// The content-addressing boundary marker: a SUFFIX on a table-dir segment (`…/<uuid>@cas@`), not a
/// path segment. It marks where the mirrored ClickHouse path ends and the content-addressed archive
/// begins — like a `.zip` extension (`foo.zip/inner/file`). `@` is S3-safe and never occurs in
/// ClickHouse uuids, part names, detached prefixes, projection names, or column files, so it cannot
/// collide with real path data. Namespace discovery comes from the catalog, not path classification.
inline constexpr std::string_view kCasArchiveSuffix = "@cas@";

/// Compose the mirrored content-addressed archive path for a table identifier as the parser reports
/// it. Atomic tables report the bare `<uuid>` → reconstruct `store/<u3>/<uuid>@cas@` (u3 = first 3
/// chars, matching ClickHouse's store fanout). Non-Atomic tables report the full joined
/// `data/<db>/<tbl>` path → append `@cas@` to it verbatim. The `@cas@` suffix lands on the
/// table-dir (last) segment in both cases. Pure; no ClickHouse dependency.
std::string mirroredArchiveNamespace(const std::string & table_uuid);

/// Reserved table-level subdirectory: TABLE_DIR/deduplication_logs/FILE is structurally
/// indistinguishable from a part file in the Atomic layout, so the name is reserved — never a part
/// dir; its contents are table-level verbatim files. ClickHouse part names never take this form.
inline constexpr std::string_view kDeduplicationLogsDirName = "deduplication_logs";

/// The result of splitting a part path. `table_uuid` is a bare UUID for an Atomic table and the
/// joined `data/<db>/<tbl>` path for a non-Atomic table. For detached or moving paths,
/// `part_name` is the reserved directory name and `file` begins with the actual part directory;
/// this preserves the on-disk shape expected by the routing layer. FREEZE paths additionally
/// retain both the backup name and the literal shadow table directory for shadow-specific routing.
struct PartFilePath
{
    std::string table_uuid;
    std::string part_name;
    std::string file; /// empty when the path is a part directory
    /// Set to the backup name when the path is a FREEZE target shadow/<backup_name>/.../<part>[/<file>].
    /// Empty for a normal live-part path.
    std::string backup_name;
    /// Set (alongside backup_name) for a FREEZE target: the LITERAL shadow table dir under the disk
    /// root excluding the part and file (shadow/<backup>/store/<uuid[:3]>/<uuid>). Empty otherwise.
    std::string shadow_table_dir;
};

/// Parse a disk-relative ClickHouse path to its (table, part, in-part file) split. Anchors on the
/// Atomic <uuid[:3]>/<uuid> pair anywhere in the path (robust to a leading store/ or shadow/
/// prefix); falls back to the RIGHTMOST part-dir-shaped component for non-Atomic layouts
/// (data/db/table/part/...). Returns nullopt for the table dir or shallower.
std::optional<PartFilePath> parsePartFilePath(const std::string & path);

/// Returns the table identifier iff path is exactly a table dir: the bare <uuid> for the Atomic
/// layout, the full joined data/db/table path for non-Atomic.
std::optional<std::string> parseTableUuid(const std::string & path);

/// True iff the path is an Atomic-layout INTERMEDIATE shard directory `store/<u3>`, where <u3> is a
/// 3-character uuid prefix (the only child it has on disk is a uuid-anchored `<u3>/<uuid>` table
/// dir). This shape is ambiguous with the non-Atomic `data/<db>` fallback of parseTableUuid, so the
/// metadata router must consult it FIRST and treat `store/<u3>` as a generic intermediate dir to be
/// enumerated by a mirrored LIST — never as a non-Atomic table id.
bool isAtomicShardDir(const std::string & path);

/// Strict "this dir IS a uuid-anchored table dir" predicate: the path's LAST two components form
/// an Atomic <uuid[:3]>/<uuid> pair. Unlike parseTableUuid it rejects the non-Atomic fallback —
/// the shadow router uses it to tell a shadow TABLE dir from a shadow INTERMEDIATE dir.
bool endsWithTableUuidPair(const std::string & path);

/// True iff the path addresses a file INSIDE a part dir (content-addressed). Table-level files
/// (format_version.txt, deduplication_logs/...) and generic disk files are excluded.
bool isPartFilePath(const std::string & path);

/// The result of splitting a table-level file path. `table_uuid` uses the same Atomic versus
/// non-Atomic representation as `PartFilePath`, while `tail` preserves the complete path below
/// that table directory, including a nested `deduplication_logs/` prefix when present.
struct TableFilePath
{
    std::string table_uuid;
    std::string tail; /// path beyond the table dir, full sub-path preserved
};

/// Parse a non-part table-level file path. Returns nullopt for the bare table dir, shallower
/// paths, part files, and generic disk-root files (e.g. clickhouse_access_check_*).
std::optional<TableFilePath> parseTableFilePath(const std::string & path);

/// True iff the path's FIRST component is the reserved FREEZE shadow root. Routed BEFORE the
/// live-table branches (a shadow table dir also satisfies parseTableUuid).
bool isShadowPath(const std::string & path);

/// Return the number of underlying `splitNonEmpty` invocations on the current thread. This is an
/// observability seam for verifying that repeated classifiers reuse the thread-local split cache.
size_t splitCacheMissesForTest();

/// Clear the current thread's split cache and reset its miss counter. This is intended for tests;
/// production callers must not rely on cache state surviving between operations.
void resetSplitCacheForTest();

}
