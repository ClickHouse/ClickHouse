#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <base/types.h>
#include <cstdint>
#include <string_view>
#include <vector>

namespace DB::Cas
{

/// The outcome log records observations made while settling GC candidates. Each log belongs to one
/// attempt-scoped generation, round, and shard at
/// `gc/gen/{g}/attempt/{a}/outcomes/{round}/{shard}`. It contains the results of exact-token deletes
/// for entries that were already published as `delete_pending`, as well as candidates spared when
/// the one-pass merge found a live in-degree. The log is written before the round's single state CAS;
/// An existing durable log is adopted on replay rather than treated as an error on a byte
/// difference. The uncompressed payload is a header line, one flat JSON record per entry in insertion
/// order, and an `{"n":count}` trailer. `FormatId::GcOutcomes` stores the sealed payload in one zstd
/// frame, so its object-storage key has the `.zst` suffix.
enum class OutcomeKind : uint8_t
{
    Deleted = 1,    /// The exact-token delete succeeded.
    Absent = 2,     /// The object was already absent, for example after a prior round's delete.
    Replaced = 3,   /// A 412 showed that a writer recreated the object with a new token.
    Spared = 4,     /// The merge found a positive in-degree, so the candidate was kept alive.
};

/// Canonical wire word for one `OutcomeKind`.
std::string_view outcomeKindToWireWord(OutcomeKind outcome);

/// Its fail-closed inverse: an unknown word is `CORRUPTED_DATA`.
OutcomeKind outcomeKindFromWireWord(std::string_view w);

/// One observation about a blob incarnation considered by GC. `token` identifies the exact
/// incarnation that GC examined, while `ref` identifies the content address; retaining both lets
/// replay and inspection distinguish an absent object from a replacement that won a race with GC.
struct OutcomeEntry
{
    ObjectKind kind = ObjectKind::Blob;
    BlobRef ref{};
    PersistedEtag token;
    OutcomeKind outcome = OutcomeKind::Spared;
};

/// The ordered records for one GC outcome object. Encoding preserves this insertion order because
/// the log is observation-bearing rather than a canonical deterministic artifact; decoding returns
/// only after the trailer count matches the records that were read.
struct OutcomeLog
{
    std::vector<OutcomeEntry> entries;
};

/// Encodes a log as the uncompressed `GcOutcomes` text payload. The caller is responsible for
/// applying the format registry's compression policy before storing the returned bytes.
String encodeOutcomeLog(const OutcomeLog & log);

/// Decodes and validates a `GcOutcomes` text payload. The header, required record fields, supported
/// enum words, line boundaries, trailer count, and end-of-object condition are checked; malformed
/// input raises `CORRUPTED_DATA`.
OutcomeLog decodeOutcomeLog(std::string_view data);

}
