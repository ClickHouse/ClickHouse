#pragma once

#include <cstddef>
#include <cstdint>

/** Native C++ port of the pcodec ("pco") format constants.
  *
  * Ported from pcodec v1.0.2 (tmp/pcodec_ref/pco/src/constants.rs). The values here are part of
  * the wire-format specification and MUST match the reference implementation bit-for-bit, so the
  * codec interoperates with `.pco` streams produced by the reference Rust library.
  *
  * See docs/format.md in the reference repository for the authoritative format description.
  */
namespace DB::Pcodec
{

/// In the reference these are `Bitlen = u32`, `Weight = u32`, `DeltaLookback = u32`.
using Bitlen = uint32_t;
using Weight = uint32_t;
using AnsState = uint32_t;
using Symbol = uint32_t;
using DeltaLookback = uint32_t;

/// Bit widths of the various header fields (see metadata layout in docs/format.md).
inline constexpr Bitlen BITS_TO_ENCODE_ANS_SIZE_LOG = 4;
inline constexpr Bitlen BITS_TO_ENCODE_MODE_VARIANT = 4;
inline constexpr Bitlen BITS_TO_ENCODE_DELTA_ENCODING_VARIANT = 4;
inline constexpr Bitlen BITS_TO_ENCODE_DELTA_ENCODING_ORDER = 3;
inline constexpr Bitlen BITS_TO_ENCODE_DELTA_CONV_QUANTIZATION = 5;
inline constexpr Bitlen BITS_TO_ENCODE_DELTA_CONV_N_WEIGHTS = 5;
inline constexpr Bitlen BITS_TO_ENCODE_DELTA_LOOKBACK_WINDOW_N_LOG = 5;
inline constexpr Bitlen BITS_TO_ENCODE_DELTA_LOOKBACK_STATE_N_LOG = 4;
inline constexpr Bitlen BITS_TO_ENCODE_N_BINS = 15;
inline constexpr Bitlen BITS_TO_ENCODE_QUANTIZE_K = 8;
inline constexpr Bitlen BITS_TO_ENCODE_DICT_LEN = 25;

/// The bit reader may read up to one extra `u64` (8 bytes) past the logical end, plus 1 for good
/// measure. Any buffer handed to a `BitReader` must have at least this many slack bytes.
inline constexpr size_t OVERSHOOT_PADDING = 9;

/// Cutoffs and legal parameter values.
inline constexpr Bitlen MAX_ANS_BITS = 14;
inline constexpr size_t MAX_ANS_BYTES = (MAX_ANS_BITS + 7) / 8;
inline constexpr Bitlen LIMITED_UNOPTIMIZED_BINS_LOG = 6;
inline constexpr size_t MAX_COMPRESSION_LEVEL = 12;
inline constexpr size_t MAX_CONSECUTIVE_DELTA_ORDER = 7;
inline constexpr size_t MAX_CONV1_DELTA_ORDER = 32;
inline constexpr Bitlen MAX_CONV1_DELTA_QUANTIZATION = (1u << BITS_TO_ENCODE_DELTA_CONV_QUANTIZATION) - 1;
inline constexpr size_t MAX_ENTRIES = 1uz << 24;
inline constexpr Bitlen MAX_SUPPORTED_PRECISION = 128;
inline constexpr size_t MAX_SUPPORTED_PRECISION_BYTES = MAX_SUPPORTED_PRECISION / 8;
inline constexpr double MULT_REQUIRED_BITS_SAVED_PER_NUM = 0.5;
inline constexpr double QUANT_REQUIRED_BITS_SAVED_PER_NUM = 1.5;
inline constexpr Bitlen CLASSIC_MEMORIZABLE_BINS_LOG = 8;

/// Defaults.
inline constexpr size_t DEFAULT_COMPRESSION_LEVEL = 8;
inline constexpr size_t DEFAULT_MAX_PAGE_N = 1uz << 18;

/// Core parts of the format specification.
inline constexpr size_t ANS_INTERLEAVING = 4;
/// The count of numbers per batch, the smallest unit of decompression. Only the final batch in
/// each page may have fewer numbers than this.
inline constexpr size_t FULL_BATCH_N = 256;

/// Enough for one full batch of latents (used to size scratch buffers).
inline constexpr size_t MAX_BATCH_LATENT_VAR_SIZE
    = FULL_BATCH_N * (MAX_SUPPORTED_PRECISION_BYTES + MAX_ANS_BYTES) + OVERSHOOT_PADDING;

/// Number-type bytes, identifying the element type in a standalone `.pco` stream
/// (see data_types/{unsigned,signed,float}.rs).
enum class NumberTypeByte : uint8_t
{
    U32 = 1,
    U64 = 2,
    I32 = 3,
    I64 = 4,
    F32 = 5,
    F64 = 6,
    U16 = 7,
    I16 = 8,
    F16 = 9,
    U8 = 10,
    I8 = 11,
};

/// Mode variants (4-bit field at the start of chunk metadata).
enum class ModeVariant : uint8_t
{
    Classic = 0,
    IntMult = 1,
    FloatMult = 2,
    FloatQuant = 3,
    Dict = 4,
};

/// Delta-encoding variants (4-bit field in chunk metadata).
enum class DeltaEncodingVariant : uint8_t
{
    None = 0,
    Consecutive = 1,
    Lookback = 2,
    Conv1 = 3,
};

}
