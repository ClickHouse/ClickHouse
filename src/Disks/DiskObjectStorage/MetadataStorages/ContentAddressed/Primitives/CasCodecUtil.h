#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/ReadBuffer.h>
#include <Common/Exception.h>
#include <string_view>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::Cas
{

/// Shared low-level byte-encoding helpers for CAS: the big-endian `UInt128` wire form, exact reads,
/// and validation of identifiers embedded in persisted data. They remain independent of any
/// particular object format.

/// On-disk UInt128 wire form: the 16-byte big-endian representation used by raw CAS byte fields and
/// key components. It is FROZEN — changing the bytes breaks every object already written. (The
/// lowercase-hex form lives in `CasTypes.h` as `u128ToHex` / `hexToU128` and is out of scope here.)
///
/// Converts `v` to the frozen 16-byte big-endian representation used in raw byte fields and keys.
inline std::string u128ToBytesBE(const UInt128 & v)
{
    std::string out(16, '\0');
    for (int i = 0; i < 16; ++i)
        out[i] = static_cast<char>(static_cast<UInt8>(v >> (8 * (15 - i))));
    return out;
}

/// Parses a frozen 16-byte big-endian value. `what` identifies the containing field in corruption
/// diagnostics; any other length is malformed persisted data and raises `CORRUPTED_DATA`.
inline UInt128 u128FromBytesBE(const std::string & b, std::string_view what)
{
    if (b.size() != 16)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS {}: big-endian UInt128 field must be 16 bytes, got {}", what, b.size());
    UInt128 v = 0;
    for (int i = 0; i < 16; ++i)
        v = (v << 8) | static_cast<UInt8>(b[i]);
    return v;
}

/// Read exactly `n` raw bytes. The bounds check MUST precede the allocation: `n` typically comes
/// from a length field just read off the wire, so on corrupted input it can be huge (a u32 field
/// admits 4 GiB) — allocating first would mean a multi-GiB transient allocation, which under a
/// memory tracker surfaces as MEMORY_LIMIT_EXCEEDED instead of the pinned CORRUPTED_DATA.
/// Comparing against `available` as the exact remainder is valid because all CAS codec decoding
/// reads from `ReadBufferFromMemory`: the whole object is in memory, so `available` is exactly
/// the number of bytes left.
/// Throws `CORRUPTED_DATA` before allocating when the encoded object cannot contain `n` bytes.
inline String readFixedBytes(ReadBuffer & in, size_t n)
{
    if (n > in.available())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS codec: truncated encoded data: need {} bytes, {} available", n, in.available());
    String s(n, '\0');
    in.readStrict(s.data(), n);
    return s;
}

/// Canonical clean relative path for ref/file names: non-empty, no NUL byte, no backslash, and no
/// segment that is empty (rejects a leading/trailing/doubled '/'), ".", or "..". Names in this
/// family originate from part names -- a NUL byte is never legitimate there, so it fails closed
/// rather than being silently truncated or passed through.
/// Returns true only for a non-empty relative path whose slash-separated components are all normal
/// names; it does not normalize or rewrite the input.
inline bool isCanonicalRefName(std::string_view name)
{
    if (name.empty() || name.find('\0') != std::string_view::npos || name.find('\\') != std::string_view::npos)
        return false;
    size_t start = 0;
    while (true)
    {
        const size_t end = name.find('/', start);
        const std::string_view segment
            = name.substr(start, end == std::string_view::npos ? std::string_view::npos : end - start);
        if (segment.empty() || segment == "." || segment == "..")
            return false;
        if (end == std::string_view::npos)
            break;
        start = end + 1;
    }
    return true;
}

/// Throws CORRUPTED_DATA naming both `caller` (the codec, e.g. "RefLogTxn") and `what` (the field,
/// e.g. "set_published_at ref_name") when `name` fails `isCanonicalRefName`.
/// On success, the input is unchanged; on failure, no partial normalization is attempted.
inline void checkCanonicalRefName(std::string_view name, std::string_view caller, std::string_view what)
{
    if (!isCanonicalRefName(name))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "{}: {} is not a canonical clean relative path: '{}'", caller, what, name);
}

/// `ManifestRef` field validity, shared by the ref codecs (`CasRefLogFormat`,
/// `CasRefSnapshotFormat`): `writer_epoch`/`build_sequence` nonzero, `manifest_ordinal` in
/// `[1, kMaxManifestOrdinal]` -- the same range `manifestOrdinalFileName` (`CasManifestId.h`) enforces
/// at key-construction time. Throws CORRUPTED_DATA naming both `caller` (the codec) and `what` (the
/// field, e.g. "set_published_at manifest_ref").
/// This keeps the value-level invariant aligned with the range enforced by manifest-key construction
/// before either a codec encoder or decoder accepts the reference.
inline void checkManifestRef(const ManifestRef & ref, std::string_view caller, std::string_view what)
{
    if (ref.writer_epoch == 0 || ref.build_sequence == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "{}: {} manifest_ref writer_epoch/build_sequence must both be nonzero, got {}-{}",
            caller, what, ref.writer_epoch, ref.build_sequence);
    if (ref.manifest_ordinal == 0 || ref.manifest_ordinal > kMaxManifestOrdinal)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "{}: {} manifest_ref manifest_ordinal {} out of range", caller, what, ref.manifest_ordinal);
}

}
