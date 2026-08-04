#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/HashingReadBuffer.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <memory>
#include <string_view>

namespace DB::Cas
{

/// Row tags for the sealed source-edge run. These byte values are part of the source-edge payload
/// format, shared by this codec and the GC fold that interprets the rows.
///
/// Source-edge rows use `source_id == 0` as a sentinel key. A real active edge must never use that key;
/// both sentinel tags are restricted to it. `kZeroMarker` describes a zero transition for the current
/// generation and is dropped when the row is carried forward. `kCondemned` carries the condemned
/// incarnation at the sentinel key across generations until settlement; its payload contains the full
/// deletion token and other condemned-row state. A condemned row subsumes the zero marker for that
/// generation.
constexpr char kEdgeActive = 0x01;
constexpr char kZeroMarker = 0x00;
constexpr char kCondemned  = 0x02;

/// The `cas_run` codec represents the GC source-edge in-degree data plane as sorted NDJSON. This is
/// the `RecordStream` family
/// (`FormatId::RunFile`): unbounded-cardinality sorted records, `object_cap = 0` (NEVER materialized
/// whole — streamed one line at a time over a `ReadBuffer`), `line_cap = 4 KiB`, `PinnedRaw` (no
/// compression) + `Strict` (byte-deterministic for `putDeterministicArtifact` adoption).
///
/// This file is backend-free: it accepts caller-owned `ReadBuffer`/`WriteBuffer` objects and never
/// includes backend or GC subsystem headers. The GC layer owns the stream lifetime and the bridge to
/// packed keys and condemned rows; this codec owns only the durable text representation and its
/// identifier-layer types. Keeping that boundary physical prevents storage or GC dependencies from
/// leaking into the format implementation.
///
/// File shape:
///   {"type":"cas_run","v":3,"kind":"source_edge"}                      header line (type + v + kind gate)
///   {"b":"01<digest-hex>","s":"<32hex>","m":"edge"}                    an active-edge / zero-marker row
///   {"b":"01<digest-hex>","s":"00000000000000000000000000000000","m":"condemned","pend":false,"tt":"etag","tv":"...","sz":123,"cr":"7","mc":false}
///   {"n":184267}                                                       trailer: record count
///
/// The record key `b` is the algo BYTE as two lowercase hex chars followed by the digest hex at the
/// algo's width; `s` is the 32-hex source id. String-sorting records by (b, s) reproduces the current
/// `(algorithm, digest, source_id)` byte order (lowercase hex preserves unsigned byte order and the
/// algorithm byte is emitted first) — the invariant the fold's two-cursor merge depends on. The row-tag word
/// `m` maps to the `kEdgeActive`/`kZeroMarker`/`kCondemned` bytes; a `condemned` row additionally
/// carries the retired incarnation (`pend`/`tt`/`tv`/`sz`/`cr`) and the durable condemn-marker
/// confirmation bit (`mc`).

/// One decoded source-edge row. All fields are identifier-layer types so the codec stays backend-free.
/// The condemned-only fields (`delete_pending`/`token`/`size`/`condemn_round`/`marker_confirmed`) are
/// meaningful only when `marker == kCondemned`.
struct SourceEdgeRecord
{
    BlobRef ref{};
    UInt128 source_id{};
    char marker = kEdgeActive;
    bool delete_pending = false;
    Token token{};
    uint64_t size = 0;
    uint64_t condemn_round = 0;
    bool marker_confirmed = false;   /// durable Condemned meta confirmed for this entry (graduation gate)
};

/// The header-line `kind` word for the only live `cas_run` kind.
inline constexpr std::string_view kSourceEdgeKindWord = "source_edge";

/// Write the typed header line `{"type":"cas_run","v":G_BUILD,"kind":"<kind>"}\n` with a fixed key
/// order for byte-determinism. The `kind` field distinguishes the record schema within the run
/// family, so a reader can reject a valid run of the wrong kind before interpreting any records.
void writeRunHeaderLine(WriteBuffer & out, std::string_view kind);

/// Read + gate the typed header line: `type` must be `cas_run`, `v` is gated by `checkCompatibility`
/// (future `v` -> `UNKNOWN_FORMAT_VERSION`), and `kind` must equal `expected_kind` (else
/// `CORRUPTED_DATA`, "unknown run kind"). This is the typed-open — all three are validated before any
/// record is interpreted.
void expectRunHeaderLine(ReadBuffer & in, std::string_view expected_kind);

/// Sorted NDJSON writer over a caller-owned `WriteBuffer` (backend-free; writes plainly — the whole-
/// object checksum is `sourceEdgeRunChecksum` over the finished bytes, which keeps this writer free of a
/// HashingWriteBuffer finalize-ordering hazard). `append` asserts records arrive in non-decreasing
/// (ref, source_id) order and throws on a regression (this replaces the old `prev_key` monotonicity
/// check). `finish` writes the `{"n":count}` trailer.
class SourceEdgeRunWriter
{
public:
    /// Write the typed source-edge header immediately. The writer borrows `out` for its entire
    /// lifetime; the caller must keep it alive and must call `finish` exactly once after the final
    /// record so the count trailer is present.
    explicit SourceEdgeRunWriter(WriteBuffer & out_);

    /// Append one record in non-decreasing `(ref, source_id)` order. Equal keys are allowed because
    /// the merge layer may produce multiple rows for the same key. A regression is a producer
    /// programming error and raises `LOGICAL_ERROR`; no partial record is written for that call.
    void append(const SourceEdgeRecord & rec);

    /// Write the record-count trailer and mark the stream finished. Calling `finish` twice raises
    /// `LOGICAL_ERROR`; appending after it is likewise rejected so a completed run cannot be extended.
    void finish();

private:
    WriteBuffer & out;
    uint64_t count = 0;
    bool have_prev = false;
    BlobRef prev_ref{};
    UInt128 prev_source_id{};
    bool finished = false;
    /// Reused line-scratch: each record is assembled here, then bulk-written to `out` in one call.
    /// `clear` keeps the buffer's capacity, so memory stays bounded by the largest line ever
    /// assembled, never by record count.
    CasJsonWriter scratch;
};

/// The whole-object seal-checksum (`RunRef.checksum`) of a stored `cas_run`: the chained CityHash128 a
/// `HashingReadBuffer` computes over ALL the object bytes. The reader accumulates the IDENTICAL hash as
/// it streams (`SourceEdgeRunReader::verifyAgainst`), so a run PUT by the producer and later read by the
/// fold agree byte-for-byte. Computed over the finished bytes on the write side (the producer already
/// holds them to PUT); streamed on the read side (the run is never materialized whole to verify).
UInt128 sourceEdgeRunChecksum(std::string_view stored_bytes);

/// Sequential streaming reader over a caller-owned `ReadBuffer` (backend-free, O(one 4 KiB line)
/// resident). The ctor reads + gates the typed header line. `next` yields records in stored order and
/// returns false once the `{"n"}` trailer is consumed (the count is verified there — the line-truncation
/// guard). Every byte read is fed through a chained CityHash128; after the trailer, `verifyAgainst`
/// compares the accumulated whole-object hash to the seal's `RunRef.checksum` and throws `CORRUPTED_DATA`
/// on a mismatch — the caller calls it after draining and BEFORE acting on the records (the deletion
/// decision). Non-movable/non-copyable (owns a `HashingReadBuffer`) — construct in place.
class SourceEdgeRunReader
{
public:
    /// Construct a reader that borrows `in`, hashes every byte read, and validates the typed header
    /// before exposing any record. The caller must drain the reader through the trailer before using
    /// `verifyAgainst`, because the seal covers the complete object rather than only decoded rows.
    explicit SourceEdgeRunReader(ReadBuffer & in_);
    SourceEdgeRunReader(const SourceEdgeRunReader &) = delete;
    SourceEdgeRunReader & operator=(const SourceEdgeRunReader &) = delete;

    /// Decode the next record in stored order. Returns `false` only after consuming and validating
    /// the count trailer and confirming that it is the final line; malformed or truncated input
    /// raises `CORRUPTED_DATA`.
    bool next(SourceEdgeRecord & rec);

    /// Compare the accumulated whole-object checksum with the seal recorded for the run. Call only
    /// after `next` has returned `false`; a mismatch raises `CORRUPTED_DATA` so callers can verify
    /// the run before acting on decoded condemned rows.
    void verifyAgainst(const UInt128 & expected);

    /// Return the whole-object hash accumulated so far. It is meaningful for seal verification only
    /// after the trailer has been consumed, when the reader has hashed every byte of the object.
    UInt128 accumulatedChecksum();

private:
    HashingReadBuffer hashing;
    uint64_t seen = 0;
    bool done = false;
};

}
