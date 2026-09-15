#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <base/types.h>
#include <string_view>

namespace DB::Cas
{

/// Read-only decode-to-JSON dispatch for `clickhouse-disks cas-inspect` (and its unit tests): given
/// any key that could live in a content-addressed pool plus the raw bytes stored at it, decode with
/// the matching codec and render the struct's fields as human-readable JSON. `layout` supplies the
/// pool's key shapes (there is no live pool/backend access here — pure function of (key, bytes)), so
/// it can be exercised directly against encoder output in unit tests, with no disk / object storage
/// involved.
///
/// Dispatch is by KEY SHAPE, most-specific first (`cas/manifests/.../NNNNNN.zst` before the
/// `cas/ns/stream/` and `cas/ns/state/` roots, `/mount` and `/fold_seal` suffixes, the
/// `gc/gen/*/attempt/*/blob_target/*/*` source-edge run segments, then the pool-wide `gc/state`
/// and `blobs/` prefix). u128 and hash fields render as lowercase hex strings (matching
/// `u128ToHex`), while a recorded incarnation's backend-native value renders as an escaped string
/// beside its dialect word. Neither is exposed as an array of bytes or a raw struct dump.
///
/// Throws `ErrorCodes::BAD_ARGUMENTS` when `key` matches none of the recognized CA layouts. Any
/// decode failure of a matched key (invalid header, corrupted bytes, future format version, ...)
/// propagates as-is from the underlying `decode*` function (typically `CORRUPTED_DATA` or
/// `UNKNOWN_FORMAT_VERSION`) — this function performs no fallback decode and swallows nothing.
String caInspectToJson(const Layout & layout, const String & key, std::string_view bytes,
                       const std::optional<NamespaceLifeId> & resolved_life = std::nullopt);

}
