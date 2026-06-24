#pragma once

#include <Storages/MergeTree/UniqueKey/Txn/UniqueKeyTxnTypes.h>

#include <string>
#include <utility>
#include <vector>

namespace DB
{
    class IDataPartStorage;
}

namespace DB::UniqueKeyTxn
{

/// Lightweight projection of `unique_key.txt` retained per active part: only
/// the scalars runtime consumers need (`creation_csn` for the csn-seed,
/// `is_marker` for backup file enumeration). The full manifest's
/// `bitmaps_created` / `forwarded` lists are not read on active parts, so they
/// are parsed transiently and discarded rather than held in RAM. A part with no
/// `unique_key.txt` (legacy / non-UK) has no `UniqueKeyPartMeta` at all.
struct UniqueKeyPartMeta
{
    CSN creation_csn = INVALID_CSN;
    bool is_marker = false;
};

/// Per-part transaction metadata, materialized from `unique_key.txt` at
/// part-attach time, plus the stateless IO surface that reads and writes it.
///
/// Stored as a single-line JSON object (sibling: `ttl.txt`):
///   {"version":1,"creation_csn":<n>,"is_marker":0|1,
///    "bitmaps_created":[{"target":"<part>","csn":<n>},...],"forwarded":[...]}
/// `read()` rejects an unknown `version` (fail-closed against a newer manifest).
///
/// `bitmaps_created` are the `(target, csn)` bitmaps OWNED by this commit;
/// recovery unlinks them on abort (one entry per touched old part).
/// `forwarded` are `(target, csn)` pairs REFERENCED but not owned — schema-only
/// today (written by the merge driver once merge support lands, no reader yet),
/// kept separate so the recovery sweep, which unlinks only `bitmaps_created`,
/// never touches a bitmap this commit does not own. Legacy parts carry no
/// manifest; `IMergeTreeDataPart::getUniqueKeyMeta` returns `nullopt` for them.
///
/// `write()` fsyncs the file BEFORE the parent dir; bitmap writers must call it
/// before any `delete_bitmap_<csn>.rbm` fsync (enforced at the commit driver).
/// `read()` throws `CORRUPTED_DATA` on parse failure.
struct UniqueKeyManifest
{
    CSN creation_csn = INVALID_CSN;
    bool is_marker = false;
    std::vector<std::pair<PartName, CSN>> bitmaps_created;
    std::vector<std::pair<PartName, CSN>> forwarded;

    /// Canonical file name inside a part directory.
    static const char * FILE_NAME;

    /// On-disk format version. Bump when the schema or field semantics change;
    /// `read()` rejects anything else so an older binary fail-closes on a newer
    /// manifest rather than misreading it.
    static constexpr UInt64 FORMAT_VERSION = 1;

    /// Read and parse `unique_key.txt` from the part storage. Throws
    /// `CORRUPTED_DATA` on parse failure or missing file (caller is expected
    /// to check existence first if the part may legitimately carry no
    /// manifest).
    static UniqueKeyManifest read(const IDataPartStorage & storage);

    /// Serialize `manifest` to `unique_key.txt` via the part storage with the
    /// documented durability ordering: write content → sync file → sync
    /// directory. Throws on durable I/O failure.
    static void write(IDataPartStorage & storage, const UniqueKeyManifest & manifest);

    /// True iff `unique_key.txt` exists in the part storage. Cheap helper for
    /// callers that need to decide whether to call `read()` vs leave the
    /// per-part metadata null (legacy parts).
    static bool exists(const IDataPartStorage & storage);
};

}
