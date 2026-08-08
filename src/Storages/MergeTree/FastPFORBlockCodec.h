#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace DB
{

/// Per-block FastPFOR codec for text-index posting lists.
///
/// Wraps FastPFOR's `CompositeCodec<SIMDFastPFor<4>, VariableByte>` (the "simdfastpfor128" scheme)
/// to compress one fixed-size block of up to BLOCK_SIZE (128) delta values at a time. This mirrors how
/// `BitpackingBlockCodec` is used as the per-block primitive, so the surrounding segment / Index Section
/// layout in `PostingListCodecImpl` is reused unchanged and the lazy cursor keeps random block access.
///
/// FastPFOR's patched-exception coding chooses a base bit-width per block and stores the few large
/// outliers separately, so a block of mostly-small deltas with occasional large gaps packs much tighter
/// than plain bit-packing (which must size every value to the maximum width).
///
/// The on-disk layout of a block is just the raw FastPFOR payload (no leading bits byte): the element
/// count comes from the segment cardinality and block index, and the byte length is derived from the
/// per-block offsets in the Index Section.
///
/// Implementation detail (pimpl): the FastPFOR headers are SIMD-only and are pulled in solely by the
/// `.cpp`, so this header stays dependency-free and the type can be named (in dead branches) even in
/// builds without FastPFOR. On such builds every method throws `SUPPORT_IS_DISABLED`.
class FastPFORBlockCodec
{
public:
    FastPFORBlockCodec();
    ~FastPFORBlockCodec();

    FastPFORBlockCodec(const FastPFORBlockCodec &) = delete;
    FastPFORBlockCodec & operator=(const FastPFORBlockCodec &) = delete;

    /// Upper bound on the encoded size of one block, in bytes. Used both to size the encode scratch and
    /// as the per-block cap when validating untrusted segment payload sizes in the lazy cursor.
    static constexpr size_t maxBlockBytes() { return 16 * 128 + 256; }

    /// Encode one block of `in` (1..128 delta values) and append it to `out`, advancing `out` past the
    /// written bytes. Returns the number of bytes written.
    size_t encode(std::span<const uint32_t> in, std::span<char> & out);

    /// Decode one block of `count` (1..128) values from `in` into `out` (which must hold at least `count`
    /// slots), advancing `in` past the consumed bytes. Returns the number of bytes consumed.
    ///
    /// Contract:
    ///   - For a tail block (`count < 128`) `in.size()` MUST equal the exact byte length of this block,
    ///     because the trailing `VariableByte` codec is length-driven. Both callers satisfy this (the
    ///     cursor passes the exact span from the Index Section; the eager decoder reaches the tail with
    ///     exactly the tail bytes remaining).
    ///   - For a full block (`count == 128`) `in.size()` may be larger than the block (the leading
    ///     `SIMDFastPFor` codec is count-driven and reads only its own self-delimited page).
    /// Throws `CORRUPTED_DATA` on any structural inconsistency in untrusted input.
    size_t decode(std::span<const std::byte> & in, size_t count, std::span<uint32_t> out);

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
