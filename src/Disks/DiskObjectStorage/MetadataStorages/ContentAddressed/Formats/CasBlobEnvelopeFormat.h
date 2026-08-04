#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace DB::Cas
{

/// Identifies the kind of content represented by an envelope. `Blob` is currently the only envelope
/// kind, but the enum remains a distinct switch-friendly type because callers use the kind as part of
/// their object and audit-event vocabulary.
enum class ObjectKind : uint8_t
{
    Blob = 1,
};

/// Describes the operation that produced an envelope's optional diagnostic provenance. It is metadata
/// for inspection and attribution only; readers do not use it to make storage or compatibility
/// decisions.
enum class ProvenanceOp : uint8_t
{
    Other = 0,
    Insert = 1,
    Merge = 2,
    Mutation = 3,
    Attach = 4,
    Repack = 5,
};

/// Optional diagnostic metadata recorded with an envelope. The fields identify when and where the
/// incarnation was created, the ClickHouse build that wrote it, and the operation that produced it;
/// none of them participates in object identity or a protocol decision.
struct Provenance
{
    uint64_t created_at_ms = 0;
    UInt128 creator_server_id{};
    uint32_t ch_version = 0;
    ProvenanceOp op = ProvenanceOp::Other;
};

/// The blob envelope is a fixed-size JSON descriptor followed by the raw payload. The JSON object
/// occupies bytes [0, json_len), ASCII spaces occupy [json_len, blob_header_len-1), and '\n' is at byte
/// blob_header_len-1. The payload therefore begins at the pool-wide constant offset
/// `blob_header_len` (256 for blob pools, a `PoolMeta` parameter), allowing the locate path to use a
/// constant shift without reading an object-specific header first. The header is also the incarnation
/// zone: it may differ between incarnations of one logical object, and each upload attempt gets a fresh
/// random u128 `tag`, which is used as the exact-token delete identity.
///
/// The envelope intentionally does not duplicate identity or unused integrity metadata. The identity
/// algorithm and digest are already present in the object key and manifest reference, `domain_id` had
/// no validating consumer, and `header_hash` had no consumer once the CityHash64 check left the
/// envelope. Writer forensics are represented
/// by `ch` and `bld`, so a separate `writer_version` is unnecessary. The `v` field is the sole format
/// compatibility gate; a reader rejects a version it does not understand before interpreting the body.
struct EnvelopeHeader
{
    ObjectKind kind = ObjectKind::Blob;
    /// Set by decode from the header `v`; encode stamps `currentCompatibilityVersion`. A reader
    /// fails closed (UNKNOWN_FORMAT_VERSION) when `v` exceeds what this build understands.
    uint32_t compatibility_version = 0;
    UInt128 incarnation_tag{};              /// `tag`
    UInt128 build_id{};                     /// `bld`
    std::optional<Provenance> provenance;   /// `ts` / `by` / `op` / `ch`
    std::optional<String> intended_ref;     /// `ref` (diagnostic; truncated on encode to fit the header)
    uint32_t header_len = 0;                /// filled by encode/decode = blob_header_len (payload offset)
    /// Test-only knob: emit an unknown `!`-critical key. Decoding the resulting header must fail
    /// closed with `UNKNOWN_FORMAT_VERSION`, exercising the compatibility rule for critical extensions.
    bool emit_unknown_critical_key = false;
};

/// Builds the fixed-length header for a pool whose `blob_header_len` is `blob_header_len` (256 for blob
/// pools). Sets `header.header_len = blob_header_len` and returns exactly that many bytes. The
/// diagnostic `ref` is the only truncatable field and is shortened, never dropped, when necessary to
/// preserve the fixed layout. The header is built without payload bytes, so an upload can stage the
/// header before the payload is streamed.
String encodeEnvelopeHeader(EnvelopeHeader & header, uint32_t blob_header_len);

/// Parses and validates the JSON descriptor, its expected `type`, and its compatibility version.
/// Derives `header_len` from the terminating '\n' and requires every preceding byte in the pad zone to
/// be an ASCII space, preventing bytes from being smuggled between the descriptor and payload. Malformed
/// type, padding, or truncation produces `CORRUPTED_DATA`; a future `v` or unknown `!`-prefixed critical
/// key produces `UNKNOWN_FORMAT_VERSION`. `object_size` is accepted for symmetry with read and GC call
/// sites; the payload length is derived downstream as `object_size - header_len`, so this function does
/// not otherwise inspect it.
EnvelopeHeader decodeEnvelopeHeader(std::string_view head_bytes, uint64_t object_size, ObjectKind expected_kind);

/// Payload starts right after the header.
inline uint64_t payloadOffset(const EnvelopeHeader & header)
{
    return static_cast<uint64_t>(header.header_len);
}

/// Map an internal `ObjectKind` to the audit-log `CasEventObjectKind`. Single source for the mapping
/// previously open-coded as a ternary at each emission site. Lives here (Formats) rather than in
/// CasEvent.h (Primitives) so the include direction Formats -> Primitives is respected and
/// `CasEvent` stays dependency-free.
inline CasEventObjectKind toEventKind(ObjectKind kind)
{
    switch (kind)
    {
        case ObjectKind::Blob: return CasEventObjectKind::Blob;
    }
}

}
