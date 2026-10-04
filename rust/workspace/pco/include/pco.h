#pragma once

#include <cstdint>

/// C FFI over the `pco` (pcodec) Rust crate, used by the ClickHouse `PCO`
/// compression codec. The crate is the upstream pcodec library, patched in the
/// ClickHouse fork to perform runtime CPU-feature dispatch of its hot loops
/// (see contrib/pcodec).
///
/// All functions operate on a single standalone `.pco` stream and are
/// thread-safe (no global mutable state beyond a cached CPU-feature tier).
extern "C"
{

/// pcodec number-type bytes. These are the exact `NUMBER_TYPE_BYTE` values of
/// the pco standalone format, so a produced stream is wire-compatible with the
/// reference pcodec implementation.
enum PcoNumberType : uint8_t
{
    PCO_U32 = 1,
    PCO_U64 = 2,
    PCO_I32 = 3,
    PCO_I64 = 4,
    PCO_F32 = 5,
    PCO_F64 = 6,
    PCO_U16 = 7,
    PCO_I16 = 8,
    PCO_F16 = 9,
    PCO_U8 = 10,
    PCO_I8 = 11,
};

/// Return codes.
enum PcoStatus : int32_t
{
    PCO_OK = 0,
    /// The compressed output did not fit in `dst_capacity`; the caller should
    /// fall back to storing the data uncompressed. Never returned by decode.
    PCO_WONT_FIT = 1,
    /// A hard error: unsupported number type, invalid argument, or (on decode)
    /// a corrupt/mismatched stream. Fails closed.
    PCO_ERROR = -1,
};

/// Compress `n_values` numbers of type `number_type` (each of the corresponding
/// width) read from `src` into `dst` (which has `dst_capacity` bytes), using the
/// given pco `compression_level` (0..=12). On success writes the produced size
/// to `*out_size` and returns `PCO_OK`. Returns `PCO_WONT_FIT` if the stream
/// would exceed `dst_capacity`, or `PCO_ERROR` on a hard error.
///
/// `src` may be unaligned. `n_values == 0` is valid (produces a header-only
/// stream).
int32_t pco_compress(
    uint8_t number_type,
    const uint8_t * src,
    uint64_t n_values,
    int32_t compression_level,
    uint8_t * dst,
    uint64_t dst_capacity,
    uint64_t * out_size);

/// Decompress a standalone pco stream `[src, src + src_size)` that is expected
/// to contain exactly `n_values` numbers, each `element_width` bytes, writing
/// them to `dst` (which has `dst_capacity` bytes). The stream is decoded using
/// its own self-described number type; the decode fails if that type's width
/// does not equal `element_width`. This is the only type information available
/// on the untyped method-byte decode path (e.g. HTTP `decompress=1`), where
/// `expected_number_type` is `0`. When the caller knows the exact number type
/// the stream must carry (a codec instance created from a concrete column
/// type), pass it as `expected_number_type` and the decode additionally fails
/// on a stream whose embedded type merely shares the width (e.g. an `I32`
/// stream for a `U32` column). Returns `PCO_OK` on success, or `PCO_ERROR` if
/// the stream is corrupt, its width or number type does not match, the element
/// count differs, or `dst_capacity` is too small. `dst` may be unaligned.
int32_t pco_decompress(
    uint32_t element_width,
    uint8_t expected_number_type,
    const uint8_t * src,
    uint64_t src_size,
    uint8_t * dst,
    uint64_t n_values,
    uint64_t dst_capacity);

} // extern "C"
