#pragma once

#include <cstdint>

namespace DB::Cas
{

/// The pool-wide floor for `blob_header_len`. One compile-time owner, read by BOTH
/// `validatePoolBlobHeaderLen` (pool creation / decode) and the blob-envelope codec, so the
/// mandatory-descriptor worst-case proof and the enforced floor can never guard different numbers.
/// The byte-for-byte worst-case derivation (`kMandatoryDescriptorWorstCase`) lives beside the
/// envelope key constants in `CasBlobEnvelopeFormat.cpp`; `CasPoolMetaFormat.cpp` records why 240
/// (rather than the bare worst case) was chosen as the floor.
inline constexpr uint64_t kMinBlobHeaderLen = 240;

}
