#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/FFOR.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/registerCompressionCodecs.h>
#include <Common/BitHelpers.h>
#include <Common/SipHash.h>
#include <DataTypes/IDataType.h>
#include <IO/BitHelpers.h>
#include <Parsers/IAST.h>
#include <base/unaligned.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <tuple>

namespace DB
{

/** Wallaby codec: adaptive lossless compression for floating-point time series.
 *
 * The codec splits the input into vectors of up to 1024 values and picks the cheapest of five
 * encodings per vector, combining the strengths of two codec families:
 *   - integerization with Frame-of-Reference or delta packing (the family of ALP,
 *     https://ir.cwi.nl/pub/33334) for values that originate from decimals,
 *   - XOR compression against a window of recent values (the family of Gorilla,
 *     https://doi.org/10.14778/2824032.2824078, and Chimp128,
 *     https://doi.org/10.14778/3551793.3551852) for everything else.
 *
 * Unlike ALP, the integerized values can be packed as zigzag deltas, which is much smaller on
 * smooth series whose neighboring values are close while the overall range is wide.
 * Unlike the streaming XOR codecs, whole-vector runs collapse into a constant vector,
 * and the XOR reference is picked from a ring of 256 recent values with an explicit index.
 *
 * Compressed payload layout (after the generic codec header):
 *
 *   u8  version                  (currently 1)
 *   u8  float_width              (4 or 8)
 *   u32 uncompressed byte size   (little endian, for validation)
 *
 * Then a sequence of vectors, each covering count = min(1024, values_remaining) values.
 * Every vector starts with a u8 mode:
 *
 *   mode 0 CONST:         float_width bytes; all count values equal it bitwise.
 *   mode 1 DECIMAL_FOR:   u8 biased scale (alpha + 32; negative alpha divides by a power of
 *                         ten, shrinking integers that end in decimal zeros), u8 bits,
 *                         u8 adjustment_bits, Int64/Int32 base (LE), u16 exception_count,
 *                         bits * 1024 / 8 bytes of FFOR-packed (q[i] - base) lanes,
 *                         adjustment_bits * 1024 / 8 bytes of FFOR-packed zigzag ULP
 *                         adjustments applied to the reconstruction in the total order of the
 *                         float bit patterns (zero when the reconstruction is bit-exact),
 *                         exception_count * { u16 position, float_width raw value }.
 *   mode 2 DECIMAL_DELTA: u8 biased scale, u8 bits, u8 adjustment_bits, Int64/Int32 first_q
 *                         (LE), u16 exception_count,
 *                         bits * 1024 / 8 bytes of FFOR-packed zigzag(q[i] - q[i-1]) lanes
 *                         (lane 0 is zero), adjustment lanes and exceptions as above.
 *   mode 3 XOR:           u32 payload byte length (LE), then a bitstream described below.
 *   mode 4 RAW:           count * float_width bytes verbatim.
 *
 * After the last vector, uncompressed_size % float_width trailing bytes are stored verbatim.
 *
 * DECIMAL modes reconstruct v[i] = Float(Float64(q[i]) / 10^alpha) (a multiplication by
 * 10^-alpha when alpha is negative) with q[i] being
 * base + unpacked[i] for FOR, or the running sum of un-zigzagged deltas starting at first_q
 * for DELTA. The encoder verifies the round trip of every value bitwise and stores values that
 * do not survive it as exceptions, patched after reconstruction, so the compression is lossless
 * independently of any floating-point subtleties. exception_count is bounded only by count:
 * the encoder trades exceptions against packed width purely by measured payload size, so on
 * mixed-precision data a scale that stores a large minority raw can legitimately win.
 * Partial vectors are padded during packing;
 * the decoder unpacks all 1024 lanes and uses the first count of them.
 *
 * XOR bitstream for count values:
 *   - 8 bits of flags; bit 0 set means the trailing-zero field is omitted in the XOR branches
 *     (chosen by the encoder for data whose XOR residues have almost no trailing zeros, where
 *     the field would be pure overhead; the center then simply extends to the lowest bit);
 *     bits 1..7 are reserved for future XOR subformats: version-1 encoders write them as zero
 *     and the decoder rejects a payload where any of them is set;
 *   - the first value is written raw (float_width * 8 bits);
 *   - for each following value, one bit selects between a run and a single value:
 *     1: run; 10 bits of length L in [1, 1023]: the previous value repeats L more times;
 *     0: single value, followed by a 2-bit branch tag:
 *        00 EQUAL:       8-bit ring index; the value equals ring[index];
 *        01 XOR_PREV:    3-bit leading-zero class, then unless omitted the exact trailing-zero
 *                        count (6 bits for Float64, 5 for Float32), then center bits;
 *                        the reference is the most recently inserted ring value;
 *        10 XOR_WINDOW:  8-bit ring index, then the same fields as XOR_PREV
 *                        with ring[index] as the reference;
 *        11 RAW:         float_width * 8 bits.
 *        center = (value XOR reference) >> trailing, of
 *        (width - lead_class_value - trailing) bits, with trailing = 0 when omitted.
 *   The ring holds the 256 most recent single values (all four branches insert, runs do not),
 *   indexed by absolute slot; both sides start from a zeroed ring, so indices into slots that
 *   were not filled yet are well-defined.
 */
class CompressionCodecWallaby final : public ICompressionCodec
{
public:
    explicit CompressionCodecWallaby(UInt8 float_width_);

    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    bool isFloatingPointTimeSeriesCodec() const override { return true; }
    bool isExperimental() const override { return true; }
    String getDescription() const override;

private:
    UInt8 float_width;
};

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr UInt8 WALLABY_CODEC_VERSION = 1;

/// Codec header: version (1) + float width (1) + uncompressed byte size (4).
constexpr UInt32 WALLABY_HEADER_SIZE = 2 * sizeof(UInt8) + sizeof(UInt32);

constexpr UInt32 WALLABY_VECTOR_VALUES = Compression::FFOR::DEFAULT_VALUES;

constexpr UInt32 WALLABY_RING_SIZE = 256;
constexpr UInt8 WALLABY_RING_INDEX_BITS = 8;
static_assert(1u << WALLABY_RING_INDEX_BITS == WALLABY_RING_SIZE);

constexpr UInt8 WALLABY_RUN_LENGTH_BITS = 10;
static_assert((1u << WALLABY_RUN_LENGTH_BITS) == WALLABY_VECTOR_VALUES);

constexpr UInt8 WALLABY_LEAD_CLASS_BITS = 3;
constexpr UInt8 WALLABY_LEAD_CLASSES = 1 << WALLABY_LEAD_CLASS_BITS;

/// The upper bound on the number of values the decimal chooser samples per vector to collect
/// candidate scales and to bound the exception count of a candidate from below.
constexpr UInt32 WALLABY_MAX_SAMPLES = 256;

/// The cost of one stored exception: its position plus the raw value.
template <typename T>
constexpr UInt32 exceptionCost() { return sizeof(UInt16) + sizeof(T); }

/// The decimal scale is signed (negative scales divide the values by a power of ten, shrinking
/// integers that end in decimal zeros); the format stores it biased into an unsigned byte.
constexpr Int32 WALLABY_ALPHA_BIAS = 32;

enum class VectorMode : UInt8
{
    Const = 0,
    DecimalFor = 1,
    DecimalDelta = 2,
    Xor = 3,
    Raw = 4,
};

/// Exact powers of ten as Float64; all values are exactly representable.
constexpr std::array<Float64, 19> WALLABY_POW10 =
{
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
};

template <typename T>
struct WallabyTraits;

template <>
struct WallabyTraits<UInt64>
{
    using FloatType = Float64;
    using SignedType = Int64;
    static constexpr UInt8 width_bits = 64;
    static constexpr UInt8 trail_bits = 6;
    static constexpr Int32 max_alpha = 18;
    static constexpr Int32 min_alpha = -18;
    static constexpr std::array<UInt8, WALLABY_LEAD_CLASSES> lead_classes{0, 8, 12, 16, 20, 24, 32, 44};
};

template <>
struct WallabyTraits<UInt32>
{
    using FloatType = Float32;
    using SignedType = Int32;
    static constexpr UInt8 width_bits = 32;
    static constexpr UInt8 trail_bits = 5;
    static constexpr Int32 max_alpha = 10;
    static constexpr Int32 min_alpha = -10;
    static constexpr std::array<UInt8, WALLABY_LEAD_CLASSES> lead_classes{0, 4, 8, 10, 12, 14, 18, 24};
};

template <typename T>
UInt8 leadClassIndex(UInt8 lead)
{
    const auto & classes = WallabyTraits<T>::lead_classes;
    UInt8 index = 0;
    for (UInt8 i = 1; i < WALLABY_LEAD_CLASSES; ++i)
        if (classes[i] <= lead)
            index = i;
    return index;
}

/// The outcome of quantizing one value, distinguishing the two reasons a value can fail, because
/// only one of them carries over to the smaller scales - see the exception bounds in the chooser.
enum class QuantizeStatus : UInt8
{
    Ok,
    /// The value needs more decimal places than this scale offers, or it is not finite at all.
    /// Either way it fails at every smaller scale too: it is an exception of all of them.
    MonotoneFailure,
    /// The scaled value leaves the domain where the integer conversion is exact. This says
    /// nothing about the smaller scales, where the same value scales to a smaller magnitude.
    DomainFailure,
};

/** Quantizes a value to the given signed decimal scale (a negative alpha divides by a power of
  * ten instead of multiplying) and verifies that the reconstruction is bitwise exact. Returns
  * the reason when the value cannot be represented this way losslessly.
  */
template <typename T>
QuantizeStatus quantizeValue(typename WallabyTraits<T>::FloatType value, Int32 alpha, typename WallabyTraits<T>::SignedType & quantized)
{
    using Traits = WallabyTraits<T>;
    using SignedType = typename Traits::SignedType;
    using FloatType = typename Traits::FloatType;

    if (!std::isfinite(value))
        return QuantizeStatus::MonotoneFailure;

    const bool negative_scale = alpha < 0;
    const Float64 power = WALLABY_POW10[negative_scale ? -alpha : alpha];
    const Float64 scaled = negative_scale ? static_cast<Float64>(value) / power : static_cast<Float64>(value) * power;
    /// Stay inside the exact llround domain.
    if (!(std::fabs(scaled) < 9.2e18))
        return QuantizeStatus::DomainFailure;

    const Int64 q = std::llround(scaled);
    if constexpr (std::is_same_v<SignedType, Int32>)
    {
        if (q < std::numeric_limits<Int32>::min() || q > std::numeric_limits<Int32>::max())
            return QuantizeStatus::DomainFailure;
    }

    const FloatType reconstructed = static_cast<FloatType>(
        negative_scale ? static_cast<Float64>(q) * power : static_cast<Float64>(q) / power);
    if (std::bit_cast<T>(reconstructed) != std::bit_cast<T>(value))
        return QuantizeStatus::MonotoneFailure;

    quantized = static_cast<SignedType>(q);
    return QuantizeStatus::Ok;
}

/// The standard total-order map of float bit patterns to unsigned integers (and back): sign
/// bit set means the bits are inverted, otherwise the sign bit is set. Adjacent floats map to
/// adjacent integers, so a difference in this domain is a distance in ULPs.
template <typename T>
ALWAYS_INLINE T orderedFromBits(T bits)
{
    constexpr T sign = T{1} << (WallabyTraits<T>::width_bits - 1);
    return (bits & sign) ? ~bits : (bits | sign);
}

template <typename T>
ALWAYS_INLINE T bitsFromOrdered(T ordered)
{
    constexpr T sign = T{1} << (WallabyTraits<T>::width_bits - 1);
    return (ordered & sign) ? (ordered ^ sign) : ~ordered;
}

template <typename T>
ALWAYS_INLINE T zigzagEncode(T difference)
{
    using SignedType = typename WallabyTraits<T>::SignedType;
    const auto signed_difference = static_cast<SignedType>(difference);
    return (static_cast<T>(signed_difference) << 1) ^ static_cast<T>(signed_difference >> (WallabyTraits<T>::width_bits - 1));
}

template <typename T>
ALWAYS_INLINE T zigzagDecode(T encoded)
{
    return (encoded >> 1) ^ (T{0} - (encoded & T{1}));
}

/** Quantizes a value to the given scale, producing the quantized integer and the zigzag ULP
  * adjustment between the reconstruction and the true value (zero when the reconstruction is
  * bit-exact). Reports a monotone failure when the reconstruction is more than 2^(width/2)
  * ULPs away — such a value lives at a completely different magnitude and is always cheaper
  * as a patched exception than as adjustment lanes — and a domain failure when no quantized
  * integer can be computed at all.
  */
template <typename T>
QuantizeStatus quantizeValueWithAdjustment(
    typename WallabyTraits<T>::FloatType value, Int32 alpha, typename WallabyTraits<T>::SignedType & quantized, T & adjustment)
{
    using Traits = WallabyTraits<T>;
    using SignedType = typename Traits::SignedType;
    using FloatType = typename Traits::FloatType;

    if (!std::isfinite(value))
        return QuantizeStatus::MonotoneFailure;

    const bool negative_scale = alpha < 0;
    const Float64 power = WALLABY_POW10[negative_scale ? -alpha : alpha];
    const Float64 scaled = negative_scale ? static_cast<Float64>(value) / power : static_cast<Float64>(value) * power;
    if (!(std::fabs(scaled) < 9.2e18))
        return QuantizeStatus::DomainFailure;

    const Int64 q = std::llround(scaled);
    if constexpr (std::is_same_v<SignedType, Int32>)
    {
        if (q < std::numeric_limits<Int32>::min() || q > std::numeric_limits<Int32>::max())
            return QuantizeStatus::DomainFailure;
    }

    const FloatType reconstructed = static_cast<FloatType>(
        negative_scale ? static_cast<Float64>(q) * power : static_cast<Float64>(q) / power);

    /// The bit-exact case dominates real decimal data; skip the adjustment arithmetic there.
    if (std::bit_cast<T>(reconstructed) == std::bit_cast<T>(value))
    {
        quantized = static_cast<SignedType>(q);
        adjustment = 0;
        return QuantizeStatus::Ok;
    }

    const T adjustment_zigzag
        = zigzagEncode<T>(orderedFromBits(std::bit_cast<T>(value)) - orderedFromBits(std::bit_cast<T>(reconstructed)));
    if (adjustment_zigzag >= (T{1} << (Traits::width_bits / 2)))
        return QuantizeStatus::MonotoneFailure;

    quantized = static_cast<SignedType>(q);
    adjustment = adjustment_zigzag;
    return QuantizeStatus::Ok;
}

/// Returns the smallest decimal scale at which the value is representable, if any. Scales are
/// signed: an integer value ending in decimal zeros quantizes at negative scales too, where it
/// is divided by a power of ten, so the search extends downward from zero while the value
/// stays bit-exact there. A scale whose reconstruction lands within a small ULP adjustment is
/// preferred over the exact scale only when it is enough scales lower that the adjustment
/// lanes (at most width/4 bits per value) cost less than the extra quantized-lane width
/// (log2(10) bits per scale step) — decimals disturbed by lossy arithmetic are bit-exact only
/// at a very fine scale, while values that are exact at a low scale vote it directly.
/// The exact scale (when one exists) goes into exact_alpha_out: the tolerant vote is a
/// preference, not a guarantee — a vector whose other values reject the voted scale still has
/// the exact scale as its provably representable fallback.
template <typename T>
std::optional<Int32> findAlpha(typename WallabyTraits<T>::FloatType value, std::optional<Int32> * exact_alpha_out = nullptr)
{
    using Traits = WallabyTraits<T>;
    constexpr T near_threshold = T{1} << (Traits::width_bits / 4);
    typename Traits::SignedType quantized;
    T adjustment;
    /// Positive zero is exactly representable at every scale; skip the downward probing.
    if (std::bit_cast<T>(value) == 0)
    {
        if (exact_alpha_out)
            *exact_alpha_out = Traits::min_alpha;
        return Traits::min_alpha;
    }
    const QuantizeStatus at_zero = quantizeValue<T>(value, 0, quantized);
    if (at_zero == QuantizeStatus::Ok)
    {
        Int32 alpha = 0;
        while (alpha > Traits::min_alpha && quantizeValue<T>(value, alpha - 1, quantized) == QuantizeStatus::Ok)
            --alpha;
        if (exact_alpha_out)
            *exact_alpha_out = alpha;
        return alpha;
    }
    Int32 near_scan_start = 0;
    if (at_zero == QuantizeStatus::DomainFailure)
    {
        /// Too large in magnitude for the integer domain at scale 0, and larger still at every
        /// positive scale: only the division of a negative scale can bring it back. The first
        /// scale inside the domain decides exactness - a value that is not exact there only
        /// loses more digits at even lower scales - but the near scan below still gets the
        /// whole in-domain range.
        Int32 alpha = -1;
        for (; alpha >= Traits::min_alpha; --alpha)
        {
            const QuantizeStatus status = quantizeValue<T>(value, alpha, quantized);
            if (status == QuantizeStatus::DomainFailure)
                continue;
            if (status == QuantizeStatus::Ok)
            {
                while (alpha > Traits::min_alpha && quantizeValue<T>(value, alpha - 1, quantized) == QuantizeStatus::Ok)
                    --alpha;
                if (exact_alpha_out)
                    *exact_alpha_out = alpha;
                return alpha;
            }
            break;
        }
        near_scan_start = std::max(alpha, Traits::min_alpha);
    }
    /// One ascending scan finds both the first scale whose reconstruction lands within the
    /// near threshold and the first bit-exact scale (the one whose adjustment is zero). The
    /// scan is the hot path of the sampling above every vector, so the two searches share
    /// their quantizations. A domain failure ends it: the scaled magnitude only grows with
    /// the scale.
    constexpr Int32 near_scale_advantage = Traits::width_bits == 64 ? 6 : 3;
    std::optional<Int32> near_alpha;
    std::optional<Int32> exact_alpha;
    for (Int32 alpha = near_scan_start; alpha <= Traits::max_alpha; ++alpha)
    {
        const QuantizeStatus status = quantizeValueWithAdjustment<T>(value, alpha, quantized, adjustment);
        if (status == QuantizeStatus::DomainFailure)
            break;
        if (status != QuantizeStatus::Ok)
            continue;
        if (!near_alpha && adjustment < near_threshold)
            near_alpha = alpha;
        if (adjustment == 0)
        {
            exact_alpha = alpha;
            break;
        }
    }
    if (exact_alpha_out)
        *exact_alpha_out = exact_alpha;
    if (near_alpha && (!exact_alpha || *near_alpha <= *exact_alpha - near_scale_advantage))
        return near_alpha;
    return exact_alpha;
}

template <typename T>
struct DecimalEncodingResult
{
    VectorMode mode;
    UInt32 payload_size;
    Int32 alpha;
};

/** Tries to encode a vector with one of the decimal modes into scratch.
  * Returns std::nullopt when the data does not fit the decimal representation, or when no
  * decimal encoding can come in under size_to_beat — the size of the best other encoding of
  * this vector (the RAW mode at worst, the already-measured XOR encoding when it ran first).
  */
template <typename T>
std::optional<DecimalEncodingResult<T>> encodeDecimal(
    const typename WallabyTraits<T>::FloatType * values, UInt32 count, char * scratch, UInt32 scratch_size, UInt32 size_to_beat,
    std::optional<Int32> hint_alpha)
{
    using Traits = WallabyTraits<T>;
    using SignedType = typename Traits::SignedType;

    constexpr UInt32 header_size = 3 * sizeof(UInt8) + sizeof(SignedType) + sizeof(UInt16);

    /// Collect the number of decimal places of a sample of the values. The sample only *guides*
    /// the candidate set; it never rejects the decimal modes. The sample positions are
    /// deterministic, so any threshold on the number of sampled failures can be defeated by a
    /// block whose only non-quantizable values sit exactly on those positions - the rejection of
    /// a scale is therefore left entirely to the measured full-vector pass below, which abandons
    /// a candidate as soon as its exceptions alone outgrow the best encoding already known for
    /// this vector. Data with no decimal structure at all costs one such bounded pass.
    /// The sample only counts *distinct* positions, so the number of sampled values that a scale
    /// cannot represent is a valid *lower* bound on its exception count - see the pruning estimate
    /// below, which is what needs the bound. The sample can be grown on demand up to
    /// WALLABY_MAX_SAMPLES exactly when a stronger bound would let the estimate prune a candidate;
    /// growing is limited to power-of-two counts, where the multiplicative sequence is a bijection
    /// and distinctness is free (that is every vector but a partial last one).
    std::array<Int8, WALLABY_MAX_SAMPLES> sampled_alphas{};
    /// The exact scale of each sampled value (the vote itself when the vote is exact): the
    /// candidate to fall back to when the value's tolerant vote fails on the rest of the vector.
    std::array<Int8, WALLABY_MAX_SAMPLES> sampled_exact_alphas{};
    UInt32 sampled_alpha_count = 0;
    /// Sampled values that no scale can represent: they are exceptions of every candidate.
    UInt32 sampled_unquantizable = 0;
    UInt32 sampled_positions = 0;
    const bool sample_can_grow = (count & (count - 1)) == 0 && count > 32;

    const auto grow_sample = [&](UInt32 target)
    {
        target = std::min({target, count, WALLABY_MAX_SAMPLES});
        for (UInt32 i = sampled_positions; i < target; ++i)
        {
            /// An odd multiplier hits every position exactly once modulo a power of two
            /// and spreads the samples uniformly for any other count.
            const UInt32 position = static_cast<UInt32>((static_cast<UInt64>(i) * 2654435761u) % count);
            std::optional<Int32> sample_exact;
            if (auto sample_alpha = findAlpha<T>(values[position], &sample_exact))
            {
                sampled_alphas[sampled_alpha_count] = static_cast<Int8>(*sample_alpha);
                sampled_exact_alphas[sampled_alpha_count] = static_cast<Int8>(sample_exact.value_or(*sample_alpha));
                ++sampled_alpha_count;
            }
            else
                ++sampled_unquantizable;
        }
        sampled_positions = std::max(sampled_positions, target);
    };

    /// The initial sample. For a count that is not a power of two the positions may repeat, so
    /// they are deduplicated to keep the failure counts a lower bound.
    if (sample_can_grow)
    {
        grow_sample(32);
    }
    else
    {
        std::array<UInt32, 32> seen_positions{};
        const UInt32 samples = std::min<UInt32>(count, 32);
        for (UInt32 i = 0; i < samples; ++i)
        {
            const UInt32 position = static_cast<UInt32>((static_cast<UInt64>(i) * 2654435761u) % count);
            bool seen = false;
            for (UInt32 j = 0; j < sampled_positions; ++j)
                seen = seen || seen_positions[j] == position;
            if (seen)
                continue;
            seen_positions[sampled_positions++] = position;
            std::optional<Int32> sample_exact;
            if (auto sample_alpha = findAlpha<T>(values[position], &sample_exact))
            {
                sampled_alphas[sampled_alpha_count] = static_cast<Int8>(*sample_alpha);
                sampled_exact_alphas[sampled_alpha_count] = static_cast<Int8>(sample_exact.value_or(*sample_alpha));
                ++sampled_alpha_count;
            }
            else
                ++sampled_unquantizable;
        }
    }

    /// Filled before use; zeroing per vector is a measurable cost on the compression path.
    /// Two buffers per quantization array: the active scratch that quantize_all writes and the
    /// preserved state of the best candidate so far. A new best swaps the pointer pairs instead
    /// of copying, and the packing phase swaps once more to make the winner active again — the
    /// winning scale is never quantized twice. The buffers live per thread, not on the frame:
    /// doubled they exceed the frame-size limit, and every vector overwrites them fully.
    struct QuantizationBuffers
    {
        alignas(64) std::array<SignedType, WALLABY_VECTOR_VALUES> quantized[2];
        alignas(64) std::array<T, WALLABY_VECTOR_VALUES> adjustments[2];
        std::array<UInt16, WALLABY_VECTOR_VALUES> exception_positions[2];
        std::array<bool, WALLABY_VECTOR_VALUES> exception_flags[2];
    };
    static thread_local QuantizationBuffers buffers;
    SignedType * quantized = buffers.quantized[0].data();
    T * adjustments = buffers.adjustments[0].data();
    UInt16 * exception_positions = buffers.exception_positions[0].data();
    bool * is_quantization_exception = buffers.exception_flags[0].data();
    SignedType * best_quantized = buffers.quantized[1].data();
    T * best_adjustments = buffers.adjustments[1].data();
    UInt16 * best_exception_positions = buffers.exception_positions[1].data();
    bool * best_exception_flags = buffers.exception_flags[1].data();
    std::array<bool, WALLABY_VECTOR_VALUES> exile_scratch; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    UInt32 exception_count = 0;
    /// Values representable at the scanned scale only up to a small ULP adjustment. The
    /// candidate comparison prices them as exceptions (the historical model), while the
    /// once-per-vector capping analysis of the winner decides whether the adjustment lanes
    /// absorb them more cheaply.
    UInt32 soft_exception_count = 0;
    T max_adjustment_zigzag = 0;

    /// No candidate may spend more on exceptions alone than the size of the best encoding of
    /// this vector known so far — initially the caller-provided bound (the RAW mode at worst),
    /// tightening as better candidates are measured. This turns the exception cap into a
    /// size-based break-even instead of a fixed count: a candidate is abandoned exactly when
    /// its exceptions alone already make it larger than something we can already do.
    UInt32 best_total_size = std::min<UInt32>(count * sizeof(T), size_to_beat);

    const auto exception_budget = [&]() -> UInt32
    {
        return best_total_size <= header_size ? 0 : (best_total_size - header_size) / exceptionCost<T>();
    };

    /// Of the exceptions of the scale scanned last, how many are exceptions of every smaller
    /// scale as well - see monotone_exceptions_at below. A scan abandoned halfway still leaves a
    /// valid count here: every position it did visit is a real position of the vector.
    UInt32 monotone_exceptions = 0;

    const auto quantize_all = [&](Int32 candidate_alpha) -> bool
    {
        const UInt32 budget = exception_budget();
        exception_count = 0;
        soft_exception_count = 0;
        max_adjustment_zigzag = 0;
        monotone_exceptions = 0;
        std::fill(is_quantization_exception, is_quantization_exception + count, false);
        SignedType previous_good = 0;
        Int32 first_good = -1;
        for (UInt32 i = 0; i < count; ++i)
        {
            SignedType q;
            T adjustment;
            const QuantizeStatus status = quantizeValueWithAdjustment<T>(values[i], candidate_alpha, q, adjustment);
            if (status == QuantizeStatus::Ok)
            {
                quantized[i] = q;
                adjustments[i] = adjustment;
                if (adjustment != 0)
                {
                    /// Near-misses do not count against the abort budget: unlike hard
                    /// exceptions they are recoverable at a few bits each.
                    ++soft_exception_count;
                    max_adjustment_zigzag = std::max(max_adjustment_zigzag, adjustment);
                }
                previous_good = q;
                if (first_good < 0)
                    first_good = static_cast<Int32>(i);
            }
            else
            {
                monotone_exceptions += status == QuantizeStatus::MonotoneFailure ? 1 : 0;
                if (exception_count == budget)
                    return false;
                exception_positions[exception_count] = static_cast<UInt16>(i);
                is_quantization_exception[i] = true;
                ++exception_count;
                /// Any placeholder works since the value is patched at decompression;
                /// the previous one keeps both FOR and DELTA packings narrow.
                quantized[i] = previous_good;
                adjustments[i] = 0;
            }
        }
        /// Leading exceptions were filled with zero placeholders before any good value was
        /// seen; a zero can inflate the Frame-of-Reference range by the whole magnitude of
        /// the data, so backfill them with the first good value instead.
        for (Int32 i = 0; i < first_good; ++i)
            quantized[i] = quantized[first_good];
        return true;
    };

    struct Packing
    {
        UInt8 bits = 0;
        UInt8 adjustment_bits = 0;
        bool use_delta = false;
        SignedType base = 0;
        UInt32 payload_size = 0;
    };

    /** Walks the vector maintaining the delta chain under a packed-width cap: a position whose
      * zigzag delta does not fit (or that is a quantization exception) is exiled — its lane
      * holds a zero delta, the chain stays where it was, and the true value is patched from an
      * exception; the next in-lane position's delta then re-synchronizes the chain. Returns the
      * number of exceptions, and optionally fills the zigzag lanes and the exiled positions for
      * the packing phase. The measuring and packing phases must agree exactly, so both use this
      * one walk.
      */
    const auto walk_delta = [&](UInt8 cap_bits, T * delta_lanes, UInt16 * exiled_positions, SignedType & base) -> UInt32
    {
        const auto delta_fits = [&](SignedType from, SignedType to) -> bool
        {
            SignedType needed;
            if (__builtin_sub_overflow(to, from, &needed))
                return false;
            const T zigzag = (static_cast<T>(needed) << 1) ^ static_cast<T>(needed >> (Traits::width_bits - 1));
            return cap_bits >= Traits::width_bits || zigzag < (T{1} << cap_bits);
        };

        /** The chain starts at the first value, so an outlier sitting there cannot be exiled the
          * way an interior one is — the chain would stay on it and every later delta would span
          * the whole distance back to it, exiling the entire vector. The head is therefore
          * exiled and the chain started one position later when the head is the value that does
          * not belong: its delta to the next value does not fit the cap, the next delta does,
          * and the delta across the head does not (so keeping the head and exiling position one
          * would not re-synchronize either). One position of lookahead decides this; the
          * opposite case, where position one is the outlier, keeps the existing behavior.
          */
        UInt32 chain_start = 0;
        if (!is_quantization_exception[0] && count > 2 && !is_quantization_exception[1] && !is_quantization_exception[2]
            && !delta_fits(quantized[0], quantized[1]) && delta_fits(quantized[1], quantized[2])
            && !delta_fits(quantized[0], quantized[2]))
            chain_start = 1;

        SignedType chain = quantized[chain_start];
        base = chain;
        UInt32 exceptions = 0;
        if (is_quantization_exception[0] || chain_start == 1)
        {
            if (exiled_positions)
                exiled_positions[exceptions] = 0;
            ++exceptions;
        }
        if (delta_lanes)
            delta_lanes[0] = 0;
        if (chain_start == 1 && delta_lanes)
            delta_lanes[1] = 0;
        for (UInt32 i = chain_start + 1; i < count; ++i)
        {
            SignedType needed = 0;
            bool fits = !is_quantization_exception[i] && !__builtin_sub_overflow(quantized[i], chain, &needed);
            T zigzag = 0;
            if (fits)
            {
                zigzag = (static_cast<T>(needed) << 1) ^ static_cast<T>(needed >> (Traits::width_bits - 1));
                fits = cap_bits >= Traits::width_bits || zigzag < (T{1} << cap_bits);
            }
            if (fits)
            {
                chain = quantized[i];
                if (delta_lanes)
                    delta_lanes[i] = zigzag;
            }
            else
            {
                if (exiled_positions)
                    exiled_positions[exceptions] = static_cast<UInt16>(i);
                ++exceptions;
                if (delta_lanes)
                    delta_lanes[i] = 0;
            }
        }
        return exceptions;
    };

    /** Chooses between Frame-of-Reference and zigzag delta packing for the quantized vector and
      * computes the payload size of the cheaper of the two. In the cheap form (allow_capping =
      * false, used by the candidate loop) the widths are the full ones and every near-miss is
      * priced as an exception. The capped form, run once per vector on the winning scale, may
      * cap either width below its maximum and exile the values that do not fit to exceptions
      * (the patching idea of PFOR), and conversely may store the near-misses as adjustment
      * lanes. The two caps interact — exiling a wide-offset value also removes the adjustment it
      * forces — so neither is chosen before the other: every candidate lane cap is scored
      * against the full objective (lanes + adjustment plan + exceptions). Per-position
      * independence makes that exact for the Frame-of-Reference offsets and for the adjustments,
      * so their caps are found by a closed-form sweep over a histogram of per-value widths,
      * while for the deltas the histogram only bounds the cost from below and an exact chain
      * walk decides, since an exiled delta partially reappears at the next position. The
      * Frame-of-Reference base stays at the vector minimum, so only large values are exiled — a
      * single small outlier still widens the lanes.
      */
    const auto measure_packing = [&](bool allow_capping) -> std::optional<Packing>
    {
        SignedType min_q = quantized[0];
        SignedType max_q = quantized[0];
        for (UInt32 i = 1; i < count; ++i)
        {
            min_q = std::min(min_q, quantized[i]);
            max_q = std::max(max_q, quantized[i]);
        }
        const T for_range = static_cast<T>(max_q) - static_cast<T>(min_q);
        const UInt8 bits_for_full = for_range == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(for_range));

        UInt32 for_width_histogram[Traits::width_bits + 1] = {};
        /// Widths of the adjacent deltas whose both values survive the quantization, and the
        /// smaller width of each pair of consecutive such deltas. Together they bound from below
        /// how many values a capped chain walk has to exile — see the lower bound below.
        UInt32 delta_width_histogram[Traits::width_bits + 1] = {};
        UInt32 delta_pair_width_histogram[Traits::width_bits + 1] = {};
        T max_zigzag = 0;
        bool delta_valid = true;
        if (allow_capping)
        {
            UInt32 previous_counted = 0;
            UInt8 previous_counted_width = 0;
            bool has_previous_counted = false;
            for (UInt32 i = 0; i < count; ++i)
            {
                if (is_quantization_exception[i])
                    continue;
                const T offset = static_cast<T>(quantized[i]) - static_cast<T>(min_q);
                ++for_width_histogram[offset == 0 ? 0 : Traits::width_bits - std::countl_zero(offset)];
                if (i > 0 && delta_valid)
                {
                    SignedType delta;
                    if (__builtin_sub_overflow(quantized[i], quantized[i - 1], &delta))
                        delta_valid = false;
                    else
                    {
                        const T zigzag = (static_cast<T>(delta) << 1) ^ static_cast<T>(delta >> (Traits::width_bits - 1));
                        max_zigzag = std::max(max_zigzag, zigzag);
                        const UInt8 zigzag_width
                            = zigzag == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(zigzag));
                        if (!is_quantization_exception[i - 1])
                        {
                            ++delta_width_histogram[zigzag_width];
                            if (has_previous_counted && previous_counted + 1 == i)
                                ++delta_pair_width_histogram[std::min(zigzag_width, previous_counted_width)];
                            has_previous_counted = true;
                            previous_counted = i;
                            previous_counted_width = zigzag_width;
                        }
                    }
                }
            }
        }
        else
        {
            for (UInt32 i = 1; i < count && delta_valid; ++i)
            {
                SignedType delta;
                if (__builtin_sub_overflow(quantized[i], quantized[i - 1], &delta))
                    delta_valid = false;
                else
                    max_zigzag = std::max(max_zigzag, (static_cast<T>(delta) << 1) ^ static_cast<T>(delta >> (Traits::width_bits - 1)));
            }
        }
        const UInt8 bits_delta_full = !delta_valid ? Traits::width_bits
            : (max_zigzag == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(max_zigzag)));

        const auto lanes_bytes = [](UInt8 w) { return Compression::FFOR::calculateBitpackedBytes(w); };

        /** Picks the width cap of the adjustment lanes from a histogram of the adjustment widths
          * of the positions that survive the lane cap, and returns it together with what it costs
          * (the lane bytes plus the exceptions the cap exiles). Adjustments are per-position
          * independent — a value whose adjustment does not fit the cap becomes an exception,
          * whatever the others do — so the closed-form scan over the histogram is exact.
          */
        const auto plan_adjustments = [&](const UInt32 * histogram, UInt8 full_bits) -> std::pair<UInt8, UInt32>
        {
            UInt8 bits = full_bits;
            UInt32 cost = lanes_bytes(full_bits);
            UInt32 outliers = 0;
            for (Int32 w = full_bits - 1; w >= 0; --w)
            {
                outliers += histogram[w + 1];
                const UInt32 candidate = lanes_bytes(static_cast<UInt8>(w)) + outliers * exceptionCost<T>();
                if (candidate < cost)
                {
                    cost = candidate;
                    bits = static_cast<UInt8>(w);
                }
            }
            return {bits, cost};
        };

        std::optional<Packing> best_packing;
        if (allow_capping || bits_for_full < Traits::width_bits)
        {
            if (!allow_capping)
            {
                const UInt32 total = header_size + lanes_bytes(bits_for_full)
                    + (exception_count + soft_exception_count) * exceptionCost<T>();
                best_packing = Packing{bits_for_full, 0, false, min_q, total};
            }
            else
            {
                /** FOR: the exile decisions are per-position independent — a value whose offset
                  * does not fit the cap is one exception, whatever the others do — so every cap
                  * width can be scored against the *full* objective, adjustment lanes included.
                  * The two caps must not be chosen in sequence: exiling a wide-offset value also
                  * removes the adjustment it forces, so a cap that looks more expensive on lanes
                  * and exceptions alone can be the cheapest once the adjustment lanes it dissolves
                  * are priced. Positions are visited in the order of their offset width (a
                  * counting sort over the histogram already built), so stepping the cap down
                  * removes each of them from the adjustment histogram exactly once and the whole
                  * sweep costs one pass over the vector plus O(width^2).
                  */
                UInt32 bucket_start[Traits::width_bits + 2] = {};
                for (UInt32 w = 0; w <= bits_for_full; ++w)
                    bucket_start[w + 1] = bucket_start[w] + for_width_histogram[w];
                UInt32 bucket_cursor[Traits::width_bits + 1] = {};
                std::copy(bucket_start, bucket_start + bits_for_full + 1, bucket_cursor);
                UInt16 by_offset_width[WALLABY_VECTOR_VALUES]; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
                UInt32 adjustment_histogram[Traits::width_bits + 1] = {};
                UInt8 adjustment_full_bits = 0;
                for (UInt32 i = 0; i < count; ++i)
                {
                    if (is_quantization_exception[i])
                        continue;
                    const T offset = static_cast<T>(quantized[i]) - static_cast<T>(min_q);
                    const UInt8 w = offset == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(offset));
                    by_offset_width[bucket_cursor[w]++] = static_cast<UInt16>(i);
                    if (max_adjustment_zigzag != 0)
                    {
                        const T adjustment = adjustments[i];
                        const UInt8 aw = adjustment == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(adjustment));
                        ++adjustment_histogram[aw];
                        adjustment_full_bits = std::max(adjustment_full_bits, aw);
                    }
                }

                UInt8 best_w = bits_for_full;
                UInt8 best_adjustment_bits = 0;
                UInt32 best_cost = std::numeric_limits<UInt32>::max();
                UInt32 exiles = 0;
                for (Int32 w = bits_for_full; w >= 0; --w)
                {
                    const auto [adjustment_bits, adjustment_cost] = plan_adjustments(adjustment_histogram, adjustment_full_bits);
                    const UInt32 cost = header_size + lanes_bytes(static_cast<UInt8>(w)) + adjustment_cost
                        + (exception_count + exiles) * exceptionCost<T>();
                    if (cost < best_cost)
                    {
                        best_cost = cost;
                        best_w = static_cast<UInt8>(w);
                        best_adjustment_bits = adjustment_bits;
                    }
                    if (w == 0)
                        break;
                    /// The cap steps down to w - 1, so the positions whose offset width is exactly
                    /// w stop fitting: they leave the adjustment plan for the exception list.
                    for (UInt32 k = bucket_start[w]; k < bucket_start[w + 1]; ++k)
                    {
                        if (max_adjustment_zigzag != 0)
                        {
                            const T adjustment = adjustments[by_offset_width[k]];
                            const UInt8 aw
                                = adjustment == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(adjustment));
                            --adjustment_histogram[aw];
                        }
                        ++exiles;
                    }
                    while (adjustment_full_bits > 0 && adjustment_histogram[adjustment_full_bits] == 0)
                        --adjustment_full_bits;
                }
                best_packing = Packing{best_w, best_adjustment_bits, false, min_q, best_cost};
            }
        }

        if (delta_valid && (allow_capping || bits_delta_full < Traits::width_bits))
        {
            /// Evaluates the delta packing at one width cap exactly: the chain walk yields the
            /// exile set, the adjustment planner covers the surviving positions.
            const auto evaluate_delta = [&](UInt8 w) -> Packing
            {
                if (!allow_capping)
                {
                    const UInt32 total = header_size + lanes_bytes(w)
                        + (exception_count + soft_exception_count) * exceptionCost<T>();
                    return Packing{w, 0, true, quantized[0], total};
                }
                UInt32 walked_exceptions = 0;
                SignedType walked_base = quantized[0];
                if (w >= bits_delta_full)
                {
                    for (UInt32 i = 0; i < count; ++i)
                        exile_scratch[i] = is_quantization_exception[i];
                    walked_exceptions = exception_count;
                }
                else
                {
                    UInt16 exiled[WALLABY_VECTOR_VALUES]; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
                    walked_exceptions = walk_delta(w, nullptr, exiled, walked_base);
                    std::fill(exile_scratch.begin(), exile_scratch.begin() + count, false);
                    for (UInt32 e = 0; e < walked_exceptions; ++e)
                        exile_scratch[exiled[e]] = true;
                }
                UInt8 adjustment_bits = 0;
                UInt32 adjustment_cost = 0;
                if (max_adjustment_zigzag != 0)
                {
                    UInt32 adjustment_histogram[Traits::width_bits + 1] = {};
                    UInt8 adjustment_full_bits = 0;
                    for (UInt32 i = 0; i < count; ++i)
                    {
                        if (exile_scratch[i])
                            continue;
                        const T adjustment = adjustments[i];
                        const UInt8 aw = adjustment == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(adjustment));
                        ++adjustment_histogram[aw];
                        adjustment_full_bits = std::max(adjustment_full_bits, aw);
                    }
                    std::tie(adjustment_bits, adjustment_cost) = plan_adjustments(adjustment_histogram, adjustment_full_bits);
                }
                const UInt32 total
                    = header_size + lanes_bytes(w) + adjustment_cost + walked_exceptions * exceptionCost<T>();
                return Packing{w, adjustment_bits, true, walked_base, total};
            };

            const Packing uncapped = evaluate_delta(bits_delta_full);
            if (!best_packing || uncapped.payload_size < best_packing->payload_size)
                best_packing = uncapped;

            if (allow_capping)
            {
                /** A cap narrower than the full width has to be verified by an exact chain walk,
                  * because the adjacent-delta histogram is not a bound in either direction: it
                  * double-counts a spike (the jump and the return are two wide adjacent deltas, but
                  * exiling the spike costs one exception, after which the chain re-synchronizes on
                  * the return), while conversely an exiled value re-widens the next delta (which
                  * now spans from further back) and that can cascade. Deciding by the histogram
                  * alone — or skipping the walk unless the histogram promises some fixed margin —
                  * would give up the "cheapest encoding per vector" contract for small wins.
                  *
                  * What the histograms do give is a lower bound on the exact cost at a cap `w`. The
                  * lane bytes and the per-exception cost are exact, and if an adjacent delta wider
                  * than `w` has both of its values surviving the quantization, the walk must exile
                  * at least one of them: had both survived the cap, the chain would step from the
                  * first to the second and that very delta would have to fit. One exiled value
                  * takes part in only two adjacent deltas, so with `V` such violations forming `R`
                  * runs of consecutive ones the walk exiles at least `max(R, ceil(V / 2))` values on
                  * top of the quantization exceptions (`R` counts one per run, and it is the exact
                  * minimum for the isolated spikes that make capping pay off in practice).
                  *
                  * So evaluate exactly every cap whose bound is below the best payload known so far,
                  * cheapest bound first, so that each measurement prunes the remaining caps. The
                  * winner is the cheapest packing over all caps, and no cap that could have won is
                  * left unwalked.
                  */
                UInt32 lower_bound[Traits::width_bits] = {};
                UInt32 violations = 0;
                UInt32 adjacent_violations = 0;
                for (Int32 w = bits_delta_full - 1; w >= 0; --w)
                {
                    violations += delta_width_histogram[w + 1];
                    adjacent_violations += delta_pair_width_histogram[w + 1];
                    const UInt32 runs = violations - std::min(violations, adjacent_violations);
                    const UInt32 exiles = std::max(runs, (violations + 1) / 2);
                    lower_bound[w] = header_size + lanes_bytes(static_cast<UInt8>(w))
                        + (exception_count + exiles) * exceptionCost<T>();
                }

                bool evaluated[Traits::width_bits] = {};
                while (true)
                {
                    Int32 cheapest = -1;
                    for (Int32 w = 0; w < bits_delta_full; ++w)
                        if (!evaluated[w] && lower_bound[w] < best_packing->payload_size
                            && (cheapest < 0 || lower_bound[w] < lower_bound[cheapest]))
                            cheapest = w;
                    if (cheapest < 0)
                        break;
                    evaluated[cheapest] = true;
                    const Packing capped = evaluate_delta(static_cast<UInt8>(cheapest));
                    if (capped.payload_size < best_packing->payload_size)
                        best_packing = capped;
                }
            }
        }

        return best_packing;
    };

    /** Candidate scales and their evaluation. A single sampled high-precision value must not
      * force a needlessly wide scale onto the whole vector, and with mixed-precision data the
      * cheapest legal scale is often one that the 32 samples never see at all. The candidate
      * set is therefore grown from three sources, each of which costs almost nothing on top
      * of work already done:
      *   - every distinct sampled alpha;
      *   - the trailing-decimal-zero counts of the quantized values at a scanned alpha A:
      *     a value whose q ends in k decimal zeros is also exactly representable at A - k, so
      *     the distinct values of A - k enumerate the scales at which some subset of the vector
      *     becomes exactly representable — including scales sampling missed (the property can
      *     fail near the precision limit, but a wrong candidate merely measures worse below).
      *     The reference alpha is scanned, and so is the first later candidate wider than it,
      *     whose quantization covers values the reference had to except;
      *   - the minimal alphas of exception values of each evaluated candidate, probed with a
      *     stride over the whole exception list: when the samples only saw a low-precision
      *     majority, this discovers the higher scale that would absorb a high-precision
      *     minority into the packed lanes, and the stride keeps that discovery independent of
      *     which exceptions happen to come first in the vector.
      * Every candidate is evaluated against the full vector and the smallest measured payload
      * wins; values that do not quantize with the winning alpha become exceptions. The most
      * frequent sampled alpha is evaluated first: it is the best single guess for the data's
      * true scale, so the pruning estimates below start from a tight baseline and the common
      * case (one homogeneous scale) stays at a single full-vector pass — in particular, a rare
      * sampled high-precision outlier no longer costs a wasted pass at its needlessly wide
      * scale before the majority scale wins.
      */
    Int32 alpha = 0;
    std::optional<Packing> best;
    /// Scalar counterparts of the swapped best-candidate buffers.
    UInt32 best_exception_count = 0;
    UInt32 best_soft_exception_count = 0;
    T best_max_adjustment_zigzag = 0;
    /// The scale domain is signed; the tracking arrays are indexed by alpha - min_alpha.
    constexpr UInt32 alpha_span = Traits::max_alpha - Traits::min_alpha + 1;
    const auto alpha_index = [](Int32 a) { return static_cast<UInt32>(a - Traits::min_alpha); };
    bool considered[alpha_span] = {};
    bool evaluated[alpha_span] = {};
    UInt32 sampled_frequency[alpha_span] = {};
    for (UInt32 i = 0; i < sampled_alpha_count; ++i)
    {
        considered[alpha_index(sampled_alphas[i])] = true;
        ++sampled_frequency[alpha_index(sampled_alphas[i])];
    }
    Int32 mode_alpha = Traits::min_alpha;
    for (Int32 a = Traits::min_alpha + 1; a <= Traits::max_alpha; ++a)
        if (sampled_frequency[alpha_index(a)] > sampled_frequency[alpha_index(mode_alpha)])
            mode_alpha = a;
    if (sampled_alpha_count == 0)
        mode_alpha = 0;

    /// The first successfully evaluated alpha (normally the most frequent sampled one); it is the
    /// scale whose quantization seeds the trailing-zero candidate generation.
    Int32 reference_alpha = 0;
    bool reference_filled = false;

    /// Per scale, the number of values that its scan found to need more decimal places than it
    /// offers. Those values are exceptions of every smaller scale as well, which is the bound the
    /// pruning below uses.
    UInt32 monotone_exceptions_at[alpha_span] = {};

    /** Lower bound on the payload of a candidate scale, deciding whether it can still beat the
      * best measured payload. It is a *bound*, not an estimate: a candidate is skipped only when
      * it provably cannot win, so the chooser never misses the cheapest encoding of a vector.
      *
      * The only term is the exception count, counted rather than extrapolated: each sampled value
      * whose minimal alpha exceeds the candidate (or that no scale can represent) is one distinct
      * position the candidate has to except. Extrapolating that count to the whole vector is not a
      * bound, because the sample positions are deterministic and a block can put its entire
      * high-precision minority on them. The packed lanes contribute nothing: nothing about their
      * width can be bounded from below from the reference scale's width. Scaling it by log2(10)
      * bits per scale step (as an earlier revision did) is an *upper* bound in the widening
      * direction and unsound in the narrowing one, since the payload takes the cheaper of the
      * Frame-of-Reference and the delta packing, and a wider scale can turn an almost-constant
      * exception-ridden vector into one-bit lanes with no exceptions at all.
      *
      * The second source of the bound is the work of the candidates already evaluated. A value
      * that needs more decimal places than some scale offers - or that is not finite - needs more
      * than any smaller scale offers too, so the number of such values seen while scanning a wider
      * scale is a lower bound on the exception count of every narrower one. This is what makes
      * full-precision data cheap again: the first scanned scale abandons its scan once its
      * exceptions outgrow the best known encoding, and that very count then prunes every narrower
      * scale without scanning it. (Failures caused by the scaled magnitude leaving the exact
      * integer domain are excluded: those do not carry over to smaller scales.)
      *
      * A bound counted over a sample of 32 values prunes little, and every unpruned candidate
      * costs two full passes over the vector. So when the current sample is not enough to prune,
      * the sample is grown (up to WALLABY_MAX_SAMPLES) and the bound recomputed: a larger sample
      * of distinct positions is a strictly stronger bound, and the growth is paid for only where
      * it can save the far more expensive full-vector passes.
      */
    const auto estimate_allows = [&](Int32 candidate) -> bool
    {
        if (!reference_filled || candidate == reference_alpha)
            return true;
        const auto exceptions_bound = [&]
        {
            UInt32 bound = sampled_unquantizable;
            for (UInt32 i = 0; i < sampled_alpha_count; ++i)
                bound += sampled_alphas[i] > candidate ? 1 : 0;
            for (Int32 wider = candidate + 1; wider <= Traits::max_alpha; ++wider)
                bound = std::max(bound, monotone_exceptions_at[alpha_index(wider)]);
            return bound;
        };
        UInt32 exceptions_lower_bound = exceptions_bound();
        /// A counted value is not necessarily a full exception of the candidate: it may be
        /// representable there up to an adjustment (its vote merely preferred a cheaper scale),
        /// and adjustment lanes absorb it at no less than the near threshold's width plus one
        /// bit. The provable floor of the counted set is therefore the cheaper of the
        /// exception list and those lanes.
        const auto bound_bytes = [](UInt32 bound)
        {
            return std::min<UInt32>(bound * exceptionCost<T>(),
                Compression::FFOR::calculateBitpackedBytes(Traits::width_bits / 4 + 1));
        };
        /// best_total_size also carries the caller-provided bound, so a candidate that cannot
        /// beat the other encoding of this vector is skipped even before any candidate wins.
        /// The sample saturates at the vector size, not only at WALLABY_MAX_SAMPLES: a partial
        /// trailing vector of 64 or 128 values (a power of two, so the sample is growable) has
        /// every position sampled after one growth step, and asking grow_sample for more would
        /// loop forever without making the bound any stronger.
        const UInt32 sample_limit = std::min<UInt32>(count, WALLABY_MAX_SAMPLES);
        while (header_size + bound_bytes(exceptions_lower_bound) < best_total_size)
        {
            if (!sample_can_grow || sampled_positions >= sample_limit)
                return true;
            grow_sample(sampled_positions * 4);
            exceptions_lower_bound = exceptions_bound();
        }
        return false;
    };

    bool candidates_pending = false;

    const auto consider_candidate = [&](Int32 candidate)
    {
        if (considered[alpha_index(candidate)])
            return;
        considered[alpha_index(candidate)] = true;
        if (!evaluated[alpha_index(candidate)])
            candidates_pending = true;
    };

    /** Candidate generation from trailing decimal zeros: a value whose quantized integer ends in
      * k decimal zeros is also exactly representable at source_alpha - k, so those scales are
      * worth measuring even when the samples never saw them. (This can miss structure for Float32
      * at scales beyond the type's decimal precision, where the quantized integers carry junk low
      * digits; the sampled alphas and the exception probes still stand.) The scan runs on the
      * quantization currently in the scratch array, so it only sees the values that the source
      * scale can represent - a wider scale evaluated later exposes structure in values that the
      * reference scale had to except, which is why it gets a scan of its own.
      */
    UInt32 trailing_zero_scans = 0;
    std::optional<Int32> trailing_zero_alpha;

    const auto generate_trailing_zero_candidates = [&](Int32 source_alpha)
    {
        ++trailing_zero_scans;
        trailing_zero_alpha = source_alpha;
        const UInt32 max_trailing_zeros = static_cast<UInt32>(source_alpha - Traits::min_alpha);
        for (UInt32 i = 0; i < count; ++i)
        {
            SignedType q = quantized[i];
            UInt32 trailing_zeros = 0;
            if (q == 0)
                trailing_zeros = max_trailing_zeros;
            else
                while (q % 10 == 0 && trailing_zeros < max_trailing_zeros)
                {
                    q /= 10;
                    ++trailing_zeros;
                }
            consider_candidate(source_alpha - static_cast<Int32>(trailing_zeros));
        }
    };

    /// Probe exception values for the scale they would need: when sampling only saw a
    /// low-precision majority, this is what discovers the higher-precision scale. The probes
    /// are spread over the whole exception list with a stride instead of taking the first
    /// few, so that a handful of very wide outliers at the front of the vector cannot hide
    /// the scale that the remaining exceptions share.
    const auto probe_exceptions = [&]()
    {
        if (exception_count == 0)
            return;
        const UInt32 probes = std::min<UInt32>(exception_count, 16);
        const UInt32 stride = std::max<UInt32>(1, exception_count / probes);
        for (UInt32 p = 0; p < probes; ++p)
        {
            const UInt32 e = std::min<UInt32>(p * stride, exception_count - 1);
            std::optional<Int32> probe_exact;
            if (auto exception_alpha = findAlpha<T>(values[exception_positions[e]], &probe_exact))
                consider_candidate(*exception_alpha);
            /// The tolerant vote of a disturbed decimal can repeat a scale it is an
            /// exception of; the exact scale is the one that absorbs it into the lanes.
            if (probe_exact)
                consider_candidate(*probe_exact);
        }
    };

    bool probed_aborted_scan = false;

    const auto evaluate_candidate = [&](Int32 candidate)
    {
        evaluated[alpha_index(candidate)] = true;

        if (!estimate_allows(candidate))
            return;

        const bool quantized_all = quantize_all(candidate);
        monotone_exceptions_at[alpha_index(candidate)] = std::max(monotone_exceptions_at[alpha_index(candidate)], monotone_exceptions);
        if (!quantized_all)
        {
            /// The samples that voted this scale preferred it over their exact scale by the
            /// tolerant vote; when the rest of the vector rejects it, their exact scales are the
            /// provably representable fallback. Without this, a vector whose only considered
            /// candidate fails here never fills a reference quantization, so neither the
            /// trailing-zero scan nor the exception probes get to run.
            for (UInt32 i = 0; i < sampled_alpha_count; ++i)
                if (sampled_alphas[i] == candidate && sampled_exact_alphas[i] != sampled_alphas[i])
                    consider_candidate(sampled_exact_alphas[i]);
            /// The exceptions recorded before the scan hit its budget are equally real values of
            /// the vector, and they are exactly the ones this scale cannot represent - the scale
            /// they need is a candidate that absorbs them into the lanes. Without this, a block
            /// whose every sampled position votes one low-precision scale (so the tolerant-vote
            /// fallback above has nothing to add) drops all record of the high-precision
            /// majority the moment its only candidate aborts, and never discovers the wider
            /// scale that wins. One aborted scan's probes are enough per vector: the strided
            /// probes sample the population of unrepresentable values, which every abandoned
            /// prefix of the same vector shares, and any candidate they seed that completes
            /// keeps probing through the path below. Data with no decimal structure at all
            /// (where every probe comes back empty and every candidate aborts) pays this
            /// bounded discovery cost once instead of once per abandoned scale.
            if (!probed_aborted_scan)
            {
                probed_aborted_scan = true;
                probe_exceptions();
            }
            return;
        }

        if (!reference_filled)
        {
            reference_filled = true;
            reference_alpha = candidate;
            generate_trailing_zero_candidates(candidate);
        }
        else if (trailing_zero_scans < 2 && trailing_zero_alpha && candidate > *trailing_zero_alpha)
        {
            /// A scale wider than the one that produced the first scan quantizes values that
            /// were exceptions back then, so their trailing-zero structure becomes visible only
            /// now. Bounded to one extra scan per vector.
            generate_trailing_zero_candidates(candidate);
        }

        probe_exceptions();

        /// The values with the widest adjustments play the same role: a high-precision minority
        /// no longer becomes exceptions (the adjustment absorbs it), but the scale that would
        /// absorb it into the packed lanes is still worth an evaluation.
        if (max_adjustment_zigzag > T{0xFF})
        {
            const UInt8 max_adjustment_width = static_cast<UInt8>(Traits::width_bits - std::countl_zero(max_adjustment_zigzag));
            UInt32 probed = 0;
            for (UInt32 i = 0; i < count && probed < 8; ++i)
            {
                if (is_quantization_exception[i] || adjustments[i] == 0)
                    continue;
                const UInt8 w = static_cast<UInt8>(Traits::width_bits - std::countl_zero(adjustments[i]));
                if (w + 2 < max_adjustment_width)
                    continue;
                ++probed;
                std::optional<Int32> probe_exact;
                if (auto adjustment_alpha = findAlpha<T>(values[i], &probe_exact))
                    consider_candidate(*adjustment_alpha);
                if (probe_exact)
                    consider_candidate(*probe_exact);
            }
        }

        /// Candidate scales must compete on their final payloads. In particular, one wide
        /// adjustment can make the uncapped adjustment lanes look expensive even though the
        /// capped plan exiles that one value and keeps the remaining narrow adjustment lanes.
        /// Comparing an uncapped candidate with a capped winner can therefore discard the
        /// cheapest encoding. `measure_packing` evaluates the complete Frame-of-Reference and
        /// delta cap search, including the adjustment plan, so its result is the exact payload
        /// for this scale.
        const auto packing = measure_packing(true);
        if (!packing)
            return;
        if (!best || packing->payload_size < best->payload_size)
        {
            best = *packing;
            alpha = candidate;
            best_total_size = std::min(best_total_size, packing->payload_size);
            std::swap(quantized, best_quantized);
            std::swap(adjustments, best_adjustments);
            std::swap(exception_positions, best_exception_positions);
            std::swap(is_quantization_exception, best_exception_flags);
            best_exception_count = exception_count;
            best_soft_exception_count = soft_exception_count;
            best_max_adjustment_zigzag = max_adjustment_zigzag;
        }
    };

    /// Consecutive vectors of a column almost always share their decimal scale, so the scale
    /// that won the previous vector is the best first guess: evaluating it first makes it the
    /// reference and lets the estimates prune most other candidates without a full pass. The
    /// most frequent sampled alpha is the fallback guess (and the second candidate on vectors
    /// where the data changed); a stale hint costs at most one extra full-vector pass. When no
    /// sample quantized at all, that fallback is alpha = 0, so a block whose non-quantizable
    /// values happen to sit on the sample positions is still measured instead of rejected.
    if (hint_alpha && *hint_alpha >= Traits::min_alpha && *hint_alpha <= Traits::max_alpha)
    {
        considered[alpha_index(*hint_alpha)] = true;
        evaluate_candidate(*hint_alpha);
    }
    if (!evaluated[alpha_index(mode_alpha)])
        evaluate_candidate(mode_alpha);

    /// Fixed-point loop: evaluating a candidate can add new candidates (from exception probing),
    /// so sweep the domain until no unevaluated candidate remains. The domain has at most
    /// max_alpha - min_alpha + 1 scales, and each is evaluated at most once.
    candidates_pending = true;
    while (candidates_pending)
    {
        candidates_pending = false;
        for (Int32 candidate = Traits::max_alpha; candidate >= Traits::min_alpha; --candidate)
        {
            if (!considered[alpha_index(candidate)] || evaluated[alpha_index(candidate)])
                continue;
            evaluate_candidate(candidate);
        }
    }
    if (!best)
        return std::nullopt;

    /// The winner's quantization was preserved by the pointer swap at its evaluation; make it
    /// the active state again (later candidates worked on the other buffer).
    std::swap(quantized, best_quantized);
    std::swap(adjustments, best_adjustments);
    std::swap(exception_positions, best_exception_positions);
    std::swap(is_quantization_exception, best_exception_flags);
    exception_count = best_exception_count;
    soft_exception_count = best_soft_exception_count;
    max_adjustment_zigzag = best_max_adjustment_zigzag;

    const UInt8 bits = best->bits;
    const bool use_delta = best->use_delta;

    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> lanes; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> packed; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)

    /// Replays the exile decisions of the measuring phase in two steps: the width cap of the
    /// quantized lanes (chain walk for DELTA, offset predicate for FOR), then the width cap of
    /// the adjustment lanes over the surviving positions; the exception list collects the final
    /// positions in ascending order.
    if (use_delta)
    {
        UInt16 exiled[WALLABY_VECTOR_VALUES]; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
        SignedType walked_base = quantized[0];
        const UInt32 walked = walk_delta(bits, lanes.data(), exiled, walked_base);
        if (walked_base != best->base)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wallaby delta chain base mismatch between measurement and packing");
        std::fill(exile_scratch.begin(), exile_scratch.begin() + count, false);
        for (UInt32 e = 0; e < walked; ++e)
            exile_scratch[exiled[e]] = true;
        for (UInt32 i = count; i < WALLABY_VECTOR_VALUES; ++i)
            lanes[i] = 0;
        Compression::FFOR::bitPack(lanes.data(), packed.data(), bits, T{0});
    }
    else
    {
        for (UInt32 i = 0; i < count; ++i)
        {
            const T offset = static_cast<T>(quantized[i]) - static_cast<T>(best->base);
            exile_scratch[i] = is_quantization_exception[i] || (bits < Traits::width_bits && offset >= (T{1} << bits));
            lanes[i] = exile_scratch[i] ? static_cast<T>(best->base) : static_cast<T>(quantized[i]);
        }
        for (UInt32 i = count; i < WALLABY_VECTOR_VALUES; ++i)
            lanes[i] = static_cast<T>(best->base);
        Compression::FFOR::bitPack(lanes.data(), packed.data(), bits, static_cast<T>(best->base));
    }

    const UInt8 adjustment_bits = best->adjustment_bits;
    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> adjustment_lanes; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> adjustment_packed; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    UInt32 write_exception_count = 0;
    for (UInt32 i = 0; i < count; ++i)
    {
        if (!exile_scratch[i] && adjustment_bits < Traits::width_bits && adjustments[i] >= (T{1} << adjustment_bits))
            exile_scratch[i] = true;
        if (exile_scratch[i])
        {
            exception_positions[write_exception_count++] = static_cast<UInt16>(i);
            adjustment_lanes[i] = 0;
        }
        else
            adjustment_lanes[i] = adjustments[i];
    }
    for (UInt32 i = count; i < WALLABY_VECTOR_VALUES; ++i)
        adjustment_lanes[i] = 0;
    Compression::FFOR::bitPack(adjustment_lanes.data(), adjustment_packed.data(), adjustment_bits, T{0});

    const UInt32 packed_bytes = Compression::FFOR::calculateBitpackedBytes(bits);
    const UInt32 adjustment_bytes = Compression::FFOR::calculateBitpackedBytes(adjustment_bits);
    const UInt32 payload_size = best->payload_size;
    if (payload_size != header_size + packed_bytes + adjustment_bytes + write_exception_count * exceptionCost<T>())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wallaby packing size mismatch between measurement and packing");
    if (payload_size > scratch_size)
        return std::nullopt;

    char * out = scratch;
    *out++ = static_cast<char>(alpha + WALLABY_ALPHA_BIAS);
    *out++ = static_cast<char>(bits);
    *out++ = static_cast<char>(adjustment_bits);
    unalignedStoreLittleEndian<SignedType>(out, best->base);
    out += sizeof(SignedType);
    unalignedStoreLittleEndian<UInt16>(out, static_cast<UInt16>(write_exception_count));
    out += sizeof(UInt16);
    memcpy(out, packed.data(), packed_bytes);
    out += packed_bytes;
    memcpy(out, adjustment_packed.data(), adjustment_bytes);
    out += adjustment_bytes;
    for (UInt32 i = 0; i < write_exception_count; ++i)
    {
        unalignedStoreLittleEndian<UInt16>(out, exception_positions[i]);
        out += sizeof(UInt16);
        unalignedStoreLittleEndian<T>(out, std::bit_cast<T>(values[exception_positions[i]]));
        out += sizeof(T);
    }

    return DecimalEncodingResult<T>{use_delta ? VectorMode::DecimalDelta : VectorMode::DecimalFor, payload_size, alpha};
}

/// Hash of the high bits of a value used to probe the ring for a reference sharing them.
template <typename T>
ALWAYS_INLINE UInt8 highBitsProbeKey(T word)
{
    constexpr UInt8 width = WallabyTraits<T>::width_bits;
    return static_cast<UInt8>((word >> (width - 8)) ^ (word >> (width - 16)));
}

/// Hash of the whole value used to probe the ring for an equal value (low-cardinality data).
template <typename T>
ALWAYS_INLINE UInt8 identityProbeKey(T word)
{
    if constexpr (sizeof(T) == 8)
        return static_cast<UInt8>((word * 0x9E3779B97F4A7C15ULL) >> 56);
    else
        return static_cast<UInt8>((word * 0x9E3779B9U) >> 24);
}

constexpr UInt32 WALLABY_LOW_PROBE_BITS = 12;

/// Low bits of a value; a reference sharing them yields an XOR result with many trailing zeros.
template <typename T>
ALWAYS_INLINE UInt16 lowBitsProbeKey(T word)
{
    return static_cast<UInt16>(word & ((1u << WALLABY_LOW_PROBE_BITS) - 1));
}

/** Encodes a vector with the XOR mode into scratch and returns the payload size in bytes,
  * or 0 when the encoding is abandoned because it provably cannot come in under
  * abandon_threshold_bits — the size of the best other encoding of this vector (the RAW mode
  * at worst, the already-measured decimal encoding when it ran first and is smaller).
  */
template <typename T>
UInt32 encodeXor(const T * words, UInt32 count, char * scratch, UInt32 scratch_size, UInt64 abandon_threshold_bits)
{
    using Traits = WallabyTraits<T>;
    constexpr UInt8 width = Traits::width_bits;

    /// Decide whether the trailing-zero field pays off: sample XOR residues of neighbors, and
    /// estimate how often a reference sharing the low bits can be found (such references produce
    /// XOR results with many trailing zeros, which only help when the field is present).
    UInt64 sampled_trail = 0;
    UInt32 sampled = 0;
    UInt32 low_bits_collisions = 0;
    {
        std::array<UInt16, 128> sample_keys{};
        UInt32 keys = 0;
        for (UInt32 i = 8; i < count && keys < sample_keys.size(); i += 8)
        {
            const T xored = words[i] ^ words[i - 1];
            if (xored != 0)
            {
                sampled_trail += static_cast<UInt8>(std::countr_zero(xored));
                ++sampled;
            }
            sample_keys[keys] = lowBitsProbeKey(words[i]);
            ++keys;
        }
        for (UInt32 a = 1; a < keys; ++a)
            for (UInt32 b = (a < 8 ? 0 : a - 8); b < a; ++b)
                if (sample_keys[a] == sample_keys[b])
                {
                    ++low_bits_collisions;
                    break;
                }
        if (keys > 0)
            low_bits_collisions = low_bits_collisions * 100 / keys;
    }
    const bool omit_trail = sampled > 0 && sampled_trail / sampled < 3 && low_bits_collisions < 25;
    const UInt8 trail_field_bits = omit_trail ? 0 : Traits::trail_bits;

    BitWriter writer(scratch, scratch_size);
    writer.writeBits(8, omit_trail ? 1 : 0);

    std::array<T, WALLABY_RING_SIZE> ring{};
    /// Probe tables: high-bits hash and whole-value hash -> ring slot of the most recent value
    /// with that hash. The former finds a reference with a similar magnitude, the latter finds
    /// an equal value, which matters on low-cardinality columns.
    std::array<UInt8, 256> probe{};
    std::array<UInt8, 256> equal_probe{};
    std::array<UInt8, (1u << WALLABY_LOW_PROBE_BITS)> low_probe{};

    UInt32 ring_position = 0;
    UInt32 newest_slot = 0;
    /// The ring distance of the last XOR_WINDOW reference. Periodic data (several interleaved
    /// series in one column) keeps referencing the same distance, so the slot at the previous
    /// distance is proposed as one more candidate — the idea comes from pcodec's lookback
    /// proposals, which include the most recently used lookbacks.
    UInt32 last_window_distance = 0;

    writer.writeBits(width, words[0]);
    ring[ring_position] = words[0];
    probe[highBitsProbeKey(words[0])] = static_cast<UInt8>(ring_position);
    equal_probe[identityProbeKey(words[0])] = static_cast<UInt8>(ring_position);
    low_probe[lowBitsProbeKey(words[0])] = static_cast<UInt8>(ring_position);
    newest_slot = ring_position;
    ring_position = (ring_position + 1) % WALLABY_RING_SIZE;
    T previous = words[0];

    /// suffix_differing[i] is the number of positions in [i, count) whose value differs from its
    /// predecessor. A value equal to its predecessor can collapse into a run almost for free, but
    /// a differing value costs at least the cheapest single-value branch (EQUAL: the '0' selector,
    /// the 2-bit tag and a ring index), so these counts give a provable lower bound on the bits
    /// the not-yet-encoded suffix must take. Filled back to front; entry 0 is never read.
    std::array<UInt16, WALLABY_VECTOR_VALUES + 1> suffix_differing; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    suffix_differing[count] = 0;
    for (UInt32 j = count; j > 1; --j)
        suffix_differing[j - 1] = static_cast<UInt16>(suffix_differing[j] + (words[j - 1] != words[j - 2] ? 1 : 0));

    UInt32 i = 1;
    while (i < count)
    {
        /// Collapse repeats of the previous value into a run.
        UInt32 run_end = i;
        while (run_end < count && words[run_end] == previous && run_end - i < WALLABY_VECTOR_VALUES - 1)
            ++run_end;
        const UInt32 run_length = run_end - i;
        if (run_length >= 2)
        {
            writer.writeBits(1, 1);
            writer.writeBits(WALLABY_RUN_LENGTH_BITS, run_length);
            i = run_end;
            continue;
        }

        const T value = words[i];

        /// Branch costs in bits, all including the leading '0' selector and the 2-bit tag.
        UInt32 best_cost = 3 + width; /// RAW
        enum class Branch : UInt8 { Equal, XorPrev, XorWindow, Raw };
        Branch best_branch = Branch::Raw;
        UInt32 best_index = 0;
        UInt8 best_class = 0;
        UInt8 best_trail = 0;

        /// O(1) candidate probes: an equal value if the ring holds one, then the predecessor
        /// and the most recent value with the same high bits.
        const UInt32 equal_slot = equal_probe[identityProbeKey(value)];
        if (ring[equal_slot] == value)
        {
            best_cost = 3 + WALLABY_RING_INDEX_BITS;
            best_branch = Branch::Equal;
            best_index = equal_slot;
        }

        const UInt32 candidates[4]
            = {newest_slot,
               probe[highBitsProbeKey(value)],
               low_probe[lowBitsProbeKey(value)],
               (newest_slot + WALLABY_RING_SIZE - last_window_distance) % WALLABY_RING_SIZE};
        for (UInt32 candidate = 0; candidate < 4 && best_branch != Branch::Equal; ++candidate)
        {
            const UInt32 slot = candidates[candidate];
            if (candidate > 0 && slot == newest_slot)
                continue;

            const T xored = value ^ ring[slot];
            if (xored == 0)
            {
                const UInt32 cost = 3 + WALLABY_RING_INDEX_BITS;
                if (cost < best_cost)
                {
                    best_cost = cost;
                    best_branch = Branch::Equal;
                    best_index = slot;
                }
                break;
            }

            const UInt8 lead = static_cast<UInt8>(std::countl_zero(xored));
            const UInt8 trail = omit_trail ? 0 : static_cast<UInt8>(std::countr_zero(xored));
            const UInt8 class_index = leadClassIndex<T>(lead);
            const UInt8 center_length = width - Traits::lead_classes[class_index] - trail;
            const bool is_prev = slot == newest_slot;
            const UInt32 cost = 3 + (is_prev ? 0 : WALLABY_RING_INDEX_BITS)
                + WALLABY_LEAD_CLASS_BITS + trail_field_bits + center_length;
            if (cost < best_cost)
            {
                best_cost = cost;
                best_branch = is_prev ? Branch::XorPrev : Branch::XorWindow;
                best_index = slot;
                best_class = class_index;
                best_trail = trail;
            }
        }

        writer.writeBits(1, 0);
        switch (best_branch)
        {
            case Branch::Equal:
                writer.writeBits(2, 0b00);
                writer.writeBits(WALLABY_RING_INDEX_BITS, best_index);
                break;
            case Branch::XorPrev:
            case Branch::XorWindow:
            {
                if (best_branch == Branch::XorPrev)
                {
                    writer.writeBits(2, 0b01);
                }
                else
                {
                    writer.writeBits(2, 0b10);
                    writer.writeBits(WALLABY_RING_INDEX_BITS, best_index);
                }
                const T xored = value ^ ring[best_index];
                const UInt8 center_length = width - Traits::lead_classes[best_class] - best_trail;
                writer.writeBits(WALLABY_LEAD_CLASS_BITS, best_class);
                if (!omit_trail)
                    writer.writeBits(Traits::trail_bits, best_trail);
                writer.writeBits(center_length, xored >> best_trail);
                if (best_branch == Branch::XorWindow)
                    last_window_distance = (newest_slot + WALLABY_RING_SIZE - best_index) % WALLABY_RING_SIZE;
                break;
            }
            case Branch::Raw:
                writer.writeBits(2, 0b11);
                writer.writeBits(width, value);
                break;
        }

        ring[ring_position] = value;
        probe[highBitsProbeKey(value)] = static_cast<UInt8>(ring_position);
        equal_probe[identityProbeKey(value)] = static_cast<UInt8>(ring_position);
        low_probe[lowBitsProbeKey(value)] = static_cast<UInt8>(ring_position);
        newest_slot = ring_position;
        ring_position = (ring_position + 1) % WALLABY_RING_SIZE;
        previous = value;
        ++i;

        /// Bail out early on data this mode does not fit: abandon the XOR encoding once even the
        /// cheapest conceivable encoding of the remaining suffix (runs for repeated values, the
        /// EQUAL branch for everything else) cannot bring the total below the best encoding
        /// already known for this vector.
        constexpr UInt32 min_single_bits = 3 + WALLABY_RING_INDEX_BITS;
        if ((i & 255) == 0
            && writer.count() + static_cast<UInt64>(suffix_differing[i]) * min_single_bits >= abandon_threshold_bits)
            return 0;
    }

    writer.flush();
    return static_cast<UInt32>((writer.count() + 7) / 8);
}

template <typename T>
void decodeXor(const char * payload, UInt32 payload_size, char * out, UInt32 count)
{
    using Traits = WallabyTraits<T>;
    constexpr UInt8 width = Traits::width_bits;

    /// The output is the decompression destination itself, which has no alignment guarantee;
    /// unaligned stores compile to plain stores on every supported platform. The decoder never
    /// reads its own output back (the previous value and the ring live in locals).
    const auto emit = [out](UInt32 position, T value) ALWAYS_INLINE
    {
        unalignedStore<T>(out + static_cast<size_t>(position) * sizeof(T), value);
    };

    BitReader reader(payload, payload_size);

    /// `BitReader` silently produces zero bits past the end of the buffer, so every read
    /// is preceded by an explicit check to make truncated payloads throw deterministically.
    const auto require_bits = [&](UInt64 bits)
    {
        if (reader.remaining() < bits)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, XOR payload is truncated");
    };

    require_bits(8 + width);
    const UInt64 flags = reader.readBits(8);
    /// Bit 0 is the only flag the version-1 encoder emits; the remaining bits are reserved
    /// for future XOR subformats and must be zero, so corrupt input throws instead of decoding.
    if (flags & ~UInt64(1))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, unknown XOR flags");
    const bool omit_trail = flags != 0;

    std::array<T, WALLABY_RING_SIZE> ring{};
    UInt32 ring_position = 0;
    UInt32 newest_slot = 0;

    T previous = static_cast<T>(reader.readBits(width));
    emit(0, previous);
    ring[ring_position] = previous;
    newest_slot = ring_position;
    ring_position = (ring_position + 1) % WALLABY_RING_SIZE;

    UInt32 produced = 1;
    while (produced < count)
    {
        require_bits(1);
        if (reader.readBit())
        {
            require_bits(WALLABY_RUN_LENGTH_BITS);
            const UInt32 run_length = static_cast<UInt32>(reader.readBits(WALLABY_RUN_LENGTH_BITS));
            if (run_length == 0 || run_length > count - produced)
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, corrupt run length");
            for (UInt32 j = 0; j < run_length; ++j)
                emit(produced + j, previous);
            produced += run_length;
            continue;
        }

        T value;
        require_bits(2);
        const UInt8 tag = static_cast<UInt8>(reader.readBits(2));
        switch (tag)
        {
            case 0b00:
                require_bits(WALLABY_RING_INDEX_BITS);
                value = ring[reader.readBits(WALLABY_RING_INDEX_BITS)];
                break;
            case 0b01:
            case 0b10:
            {
                const UInt8 trail_field_bits = omit_trail ? 0 : Traits::trail_bits;
                require_bits((tag == 0b10 ? WALLABY_RING_INDEX_BITS : 0) + WALLABY_LEAD_CLASS_BITS + trail_field_bits);
                const UInt32 slot = tag == 0b01 ? newest_slot : static_cast<UInt32>(reader.readBits(WALLABY_RING_INDEX_BITS));
                const UInt8 class_index = static_cast<UInt8>(reader.readBits(WALLABY_LEAD_CLASS_BITS));
                const UInt8 trail = omit_trail ? 0 : static_cast<UInt8>(reader.readBits(Traits::trail_bits));
                const Int32 center_length = width - Traits::lead_classes[class_index] - trail;
                if (center_length <= 0)
                    throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, corrupt center length");
                require_bits(static_cast<UInt64>(center_length));
                const T center = static_cast<T>(reader.readBits(static_cast<UInt8>(center_length)));
                value = ring[slot] ^ (center << trail);
                break;
            }
            default:
                require_bits(width);
                value = static_cast<T>(reader.readBits(width));
                break;
        }

        emit(produced, value);
        ++produced;
        ring[ring_position] = value;
        newest_slot = ring_position;
        ring_position = (ring_position + 1) % WALLABY_RING_SIZE;
        previous = value;
    }

    /// The encoder flushes with zero padding bits only, so after decoding `count` values fewer
    /// than 8 bits of the declared payload may remain, and all of them must be zero. Anything
    /// else means the payload size was inflated to hide trailing garbage inside the vector.
    const UInt64 leftover_bits = reader.remaining();
    if (leftover_bits >= 8 || (leftover_bits > 0 && reader.readBits(static_cast<UInt8>(leftover_bits)) != 0))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, trailing garbage in XOR payload");
}

template <typename T>
UInt32 compressImpl(const char * source, UInt32 values_count, char * dest)
{
    using Traits = WallabyTraits<T>;
    using FloatType = typename Traits::FloatType;

    /// Scratch buffers for candidate encodings; the XOR encoding is at most ~5% larger than raw.
    constexpr UInt32 scratch_size = WALLABY_VECTOR_VALUES * sizeof(T) + WALLABY_VECTOR_VALUES / 2 + 64;
    std::array<char, scratch_size> decimal_scratch{};
    std::array<char, scratch_size> xor_scratch{};
    alignas(64) std::array<FloatType, WALLABY_VECTOR_VALUES> values{};
    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> words{};

    char * out = dest;
    UInt32 processed = 0;

    /// Hints carried between consecutive vectors of this call: the winning mode decides which
    /// encoder runs first (so the loser starts from a tight size bound), and the winning decimal
    /// scale is evaluated first by the chooser. Both are guesses verified by measurement, so a
    /// stale hint after a data change costs speed on one vector, never correctness or size.
    /// The scale hint is dropped as soon as a vector is not won by a decimal mode: otherwise a
    /// column that starts with decimal data and then turns non-decimal would keep paying an
    /// extra full `quantize_all` pass for the dead scale on every remaining vector, not only on
    /// the vector where the data changes.
    std::optional<VectorMode> previous_winner;
    std::optional<Int32> previous_alpha;

    while (processed < values_count)
    {
        const UInt32 count = std::min<UInt32>(WALLABY_VECTOR_VALUES, values_count - processed);
        memcpy(words.data(), source + static_cast<size_t>(processed) * sizeof(T), static_cast<size_t>(count) * sizeof(T));
        memcpy(values.data(), words.data(), static_cast<size_t>(count) * sizeof(T));

        UInt32 differing = 0;
        for (UInt32 i = 1; i < count; ++i)
            differing += words[i] != words[i - 1] ? 1 : 0;

        if (differing == 0)
        {
            *out++ = static_cast<char>(VectorMode::Const);
            unalignedStoreLittleEndian<T>(out, words[0]);
            out += sizeof(T);
            processed += count;
            continue;
        }

        const UInt32 raw_size = count * sizeof(T);

        /** The decimal and XOR encodings prune each other: whichever runs first hands its
          * measured size to the other as the size to beat, so the second encoder can abandon or
          * skip candidates that provably cannot win. Every value differing from its predecessor
          * costs the XOR mode at least the cheapest single-value branch (the '0' selector, the
          * 2-bit tag and a ring index), while repeats may collapse into runs almost for free —
          * so on run-dominated vectors the XOR mode is both cheap to produce and likely to win,
          * and runs first; everywhere else the decimal chooser runs first and the XOR encoding
          * is attempted only when its lower bound leaves it a chance. The bounds must account
          * for the run collapse — a heuristic based on the decimal bit width alone would miss
          * piecewise-constant data, where the XOR mode encodes long plateaus in a few bytes
          * while the decimal modes pay per value.
          */
        constexpr UInt32 min_single_bits = 3 + WALLABY_RING_INDEX_BITS;
        const UInt64 xor_bits_lower_bound = 8 + Traits::width_bits + static_cast<UInt64>(differing) * min_single_bits;

        std::optional<DecimalEncodingResult<T>> decimal;
        UInt32 xor_size = 0;

        const bool xor_first = previous_winner == VectorMode::Xor
            || (!previous_winner.has_value() && 2 * differing < count);
        if (xor_first)
        {
            xor_size = encodeXor<T>(words.data(), count, xor_scratch.data(), scratch_size, static_cast<UInt64>(raw_size) * 8);
            const UInt32 xor_size_to_beat = xor_size == 0 ? raw_size : xor_size + static_cast<UInt32>(sizeof(UInt32));
            decimal = encodeDecimal<T>(values.data(), count, decimal_scratch.data(), scratch_size, std::min(raw_size, xor_size_to_beat), previous_alpha);
        }
        else
        {
            decimal = encodeDecimal<T>(values.data(), count, decimal_scratch.data(), scratch_size, raw_size, previous_alpha);
            const UInt32 decimal_size_to_beat = decimal ? std::min(decimal->payload_size, raw_size) : raw_size;
            if (xor_bits_lower_bound < static_cast<UInt64>(decimal_size_to_beat) * 8)
                xor_size = encodeXor<T>(words.data(), count, xor_scratch.data(), scratch_size, static_cast<UInt64>(decimal_size_to_beat) * 8);
        }

        const UInt32 decimal_size = decimal ? decimal->payload_size : std::numeric_limits<UInt32>::max();
        const UInt32 xor_total = xor_size == 0 ? std::numeric_limits<UInt32>::max() : xor_size + static_cast<UInt32>(sizeof(UInt32));

        if (decimal_size <= xor_total && decimal_size < raw_size)
        {
            *out++ = static_cast<char>(decimal->mode);
            memcpy(out, decimal_scratch.data(), decimal->payload_size);
            out += decimal->payload_size;
            previous_winner = decimal->mode;
            previous_alpha = decimal->alpha;
        }
        else if (xor_total < raw_size)
        {
            *out++ = static_cast<char>(VectorMode::Xor);
            unalignedStoreLittleEndian<UInt32>(out, xor_size);
            out += sizeof(UInt32);
            memcpy(out, xor_scratch.data(), xor_size);
            out += xor_size;
            previous_winner = VectorMode::Xor;
            previous_alpha.reset();
        }
        else
        {
            *out++ = static_cast<char>(VectorMode::Raw);
            memcpy(out, words.data(), raw_size);
            out += raw_size;
            previous_winner = VectorMode::Raw;
            previous_alpha.reset();
        }

        processed += count;
    }

    return static_cast<UInt32>(out - dest);
}

template <typename T>
UInt32 decompressImpl(const char * source, UInt32 source_size, char * dest, UInt32 values_count)
{
    using Traits = WallabyTraits<T>;
    using SignedType = typename Traits::SignedType;

    const char * src = source;
    const char * const src_end = source + source_size;
    const auto require = [&](size_t bytes)
    {
        if (static_cast<size_t>(src_end - src) < bytes)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, source is truncated");
    };

    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> lanes{};
    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> unpacked{};
    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> adjustment_lanes{};
    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> adjustments{};
    std::array<UInt16, WALLABY_VECTOR_VALUES> exception_positions{};
    std::array<T, WALLABY_VECTOR_VALUES> exception_values{};
    std::array<bool, WALLABY_VECTOR_VALUES> is_exception{};
    std::array<SignedType, WALLABY_VECTOR_VALUES> exception_quantized{};

    /// Every mode streams its values straight into the destination through unaligned stores:
    /// no mode ever reads the output back, so no intermediate vector buffer is needed and each
    /// value is written exactly once.
    char * out = dest;
    const auto emit = [&out](UInt32 position, T value) ALWAYS_INLINE
    {
        unalignedStore<T>(out + static_cast<size_t>(position) * sizeof(T), value);
    };

    UInt32 produced = 0;
    while (produced < values_count)
    {
        const UInt32 count = std::min<UInt32>(WALLABY_VECTOR_VALUES, values_count - produced);
        std::fill_n(is_exception.begin(), count, false);
        require(1);
        const UInt8 mode_byte = static_cast<UInt8>(*src++);
        if (mode_byte > static_cast<UInt8>(VectorMode::Raw))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, unknown vector mode {}", static_cast<UInt32>(mode_byte));
        const auto mode = static_cast<VectorMode>(mode_byte);

        switch (mode)
        {
            case VectorMode::Const:
            {
                require(sizeof(T));
                const T word = unalignedLoadLittleEndian<T>(src);
                src += sizeof(T);
                for (UInt32 i = 0; i < count; ++i)
                    emit(i, word);
                break;
            }
            case VectorMode::DecimalFor:
            case VectorMode::DecimalDelta:
            {
                require(3 * sizeof(UInt8) + sizeof(SignedType) + sizeof(UInt16));
                const Int32 alpha = static_cast<Int32>(static_cast<UInt8>(*src++)) - WALLABY_ALPHA_BIAS;
                const UInt8 bits = static_cast<UInt8>(*src++);
                const UInt8 adjustment_bits = static_cast<UInt8>(*src++);
                const SignedType base = unalignedLoadLittleEndian<SignedType>(src);
                src += sizeof(SignedType);
                const UInt16 exception_count = unalignedLoadLittleEndian<UInt16>(src);
                src += sizeof(UInt16);

                if (alpha < Traits::min_alpha || alpha > Traits::max_alpha || bits >= Traits::width_bits
                    || adjustment_bits > Traits::width_bits / 2 || exception_count > count)
                    throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, corrupt decimal header");

                const UInt32 packed_bytes = Compression::FFOR::calculateBitpackedBytes(bits);
                require(packed_bytes);
                memcpy(lanes.data(), src, packed_bytes);
                src += packed_bytes;

                const UInt32 adjustment_bytes = Compression::FFOR::calculateBitpackedBytes(adjustment_bits);
                require(adjustment_bytes);
                if (adjustment_bits > 0)
                {
                    memcpy(adjustment_lanes.data(), src, adjustment_bytes);
                    Compression::FFOR::bitUnpack(adjustment_lanes.data(), adjustments.data(), adjustment_bits, T{0});
                }
                src += adjustment_bytes;

                /// `DECIMAL_DELTA` cannot replay its chain until the exception positions are
                /// known. Quantization exceptions have a zero lane and leave the accumulator
                /// unchanged, while adjustment-cap exceptions retain their lane so the chain
                /// continues through the quantized value. Load and validate the exception list
                /// before reconstructing the decimal lanes, then patch the raw values after the
                /// ordinary values have been emitted.
                std::array<bool, WALLABY_VECTOR_VALUES> quantizable_exception{};
                require(exception_count * (sizeof(UInt16) + sizeof(T)));
                for (UInt32 i = 0; i < exception_count; ++i)
                {
                    const UInt16 position = unalignedLoadLittleEndian<UInt16>(src);
                    src += sizeof(UInt16);
                    const T raw = unalignedLoadLittleEndian<T>(src);
                    src += sizeof(T);
                    if (position >= count || is_exception[position])
                        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, corrupt exception position");
                    exception_positions[i] = position;
                    exception_values[i] = raw;
                    is_exception[position] = true;

                    if (mode == VectorMode::DecimalDelta)
                    {
                        SignedType quantized = 0;
                        T ignored_adjustment = 0;
                        const auto status = quantizeValueWithAdjustment<T>(
                            std::bit_cast<typename Traits::FloatType>(raw), alpha, quantized, ignored_adjustment);
                        if (status == QuantizeStatus::Ok)
                        {
                            quantizable_exception[position] = true;
                            exception_quantized[position] = quantized;
                        }
                    }
                }
                const bool negative_scale = alpha < 0;
                const Float64 scale = WALLABY_POW10[negative_scale ? -alpha : alpha];
                const auto reconstruct = [scale, negative_scale](SignedType q) ALWAYS_INLINE
                {
                    return std::bit_cast<T>(static_cast<typename Traits::FloatType>(
                        negative_scale ? static_cast<Float64>(q) * scale : static_cast<Float64>(q) / scale));
                };
                /// The adjustment shifts the reconstruction by a signed number of ULPs in the
                /// total order of the float bit patterns; zero adjustment bits (the common
                /// case) keeps the loops free of it.
                const auto adjust = [&](UInt32 i, T reconstructed_bits) ALWAYS_INLINE
                {
                    return bitsFromOrdered<T>(orderedFromBits(reconstructed_bits) + zigzagDecode(adjustments[i]));
                };
                const auto checkedAdd = [](SignedType lhs, SignedType rhs) ALWAYS_INLINE
                {
                    if ((rhs > 0 && lhs > std::numeric_limits<SignedType>::max() - rhs)
                        || (rhs < 0 && lhs < std::numeric_limits<SignedType>::min() - rhs))
                        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, decimal reconstruction overflows");
                    return static_cast<SignedType>(lhs + rhs);
                };
                if (mode == VectorMode::DecimalFor)
                {
                    Compression::FFOR::bitUnpack(lanes.data(), unpacked.data(), bits, T{0});
                    if (adjustment_bits == 0)
                        for (UInt32 i = 0; i < count; ++i)
                            emit(i, reconstruct(checkedAdd(base, static_cast<SignedType>(unpacked[i]))));
                    else
                        for (UInt32 i = 0; i < count; ++i)
                            emit(i, adjust(i, reconstruct(checkedAdd(base, static_cast<SignedType>(unpacked[i])))));
                }
                else
                {
                    Compression::FFOR::bitUnpack(lanes.data(), unpacked.data(), bits, T{0});
                    if (unpacked[0] != 0)
                        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, corrupt decimal delta lane");
                    /** `base` is the quantized value of the position the chain started from, so
                      * the accumulator may not move before that position is reached. The chain
                      * start is the first position the walk kept in the lanes; it leaves the
                      * exception list only afterwards, when the adjustment cap exiles it, and
                      * then its own quantized value is the base. So either the first
                      * non-exception position still carries the chain start's zero delta, or an
                      * earlier exception is a quantizable one whose value reconstructs the base.
                      * Anything else advances the accumulator before the chain exists.
                      */
                    UInt32 first_in_lane = 0;
                    while (first_in_lane < count && is_exception[first_in_lane])
                        ++first_in_lane;
                    if (first_in_lane < count && unpacked[first_in_lane] != 0)
                    {
                        bool base_is_an_exiled_chain_start = false;
                        for (UInt32 i = 0; i < first_in_lane; ++i)
                            base_is_an_exiled_chain_start
                                |= quantizable_exception[i] && exception_quantized[i] == base;
                        if (!base_is_an_exiled_chain_start)
                            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, delta before the decimal chain start");
                    }
                    SignedType accumulator = base;
                    for (UInt32 i = 0; i < count; ++i)
                    {
                        if (is_exception[i] && !quantizable_exception[i] && unpacked[i] != 0)
                            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, non-zero delta at a non-quantizable exception");
                        if (i > 0)
                        {
                            const SignedType delta = std::bit_cast<SignedType>(zigzagDecode(unpacked[i]));
                            const SignedType next = checkedAdd(accumulator, delta);
                            if (is_exception[i] && quantizable_exception[i] && unpacked[i] != 0 && next != exception_quantized[i])
                                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, corrupt delta at a quantizable exception");
                            if (!is_exception[i] || unpacked[i] != 0)
                                accumulator = next;
                        }
                        const T reconstructed = reconstruct(accumulator);
                        emit(i, adjustment_bits == 0 ? reconstructed : adjust(i, reconstructed));
                    }
                }

                for (UInt32 i = 0; i < exception_count; ++i)
                    emit(exception_positions[i], exception_values[i]);
                break;
            }
            case VectorMode::Xor:
            {
                require(sizeof(UInt32));
                const UInt32 payload_size = unalignedLoadLittleEndian<UInt32>(src);
                src += sizeof(UInt32);
                require(payload_size);
                decodeXor<T>(src, payload_size, out, count);
                src += payload_size;
                break;
            }
            case VectorMode::Raw:
            {
                require(static_cast<size_t>(count) * sizeof(T));
                memcpy(out, src, static_cast<size_t>(count) * sizeof(T));
                src += static_cast<size_t>(count) * sizeof(T);
                break;
            }
        }

        out += static_cast<size_t>(count) * sizeof(T);
        produced += count;
    }

    return static_cast<UInt32>(src - source);
}

}

CompressionCodecWallaby::CompressionCodecWallaby(UInt8 float_width_)
    : float_width(float_width_)
{
    setCodecDescription("Wallaby");
}

uint8_t CompressionCodecWallaby::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Wallaby);
}

void CompressionCodecWallaby::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /* ignore_aliases */ true);
    hash.update(float_width);
}

String CompressionCodecWallaby::getDescription() const
{
    return "Adaptive floating-point codec that picks per block between integerization with delta or frame-of-reference packing and windowed XOR; suitable for time series data.";
}

UInt32 CompressionCodecWallaby::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /// Worst case per vector is the RAW mode: one mode byte plus verbatim data.
    const UInt32 vectors = uncompressed_size / (WALLABY_VECTOR_VALUES * sizeof(Float32)) + 1;
    return WALLABY_HEADER_SIZE + uncompressed_size + vectors + 8;
}

UInt32 CompressionCodecWallaby::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if (source_size == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot compress with Wallaby codec, source size is 0");

    char * out = dest;
    *out++ = WALLABY_CODEC_VERSION;
    *out++ = static_cast<char>(float_width);
    unalignedStoreLittleEndian<UInt32>(out, source_size);
    out += sizeof(UInt32);

    const UInt32 values_count = source_size / float_width;
    const UInt32 trailing_bytes = source_size % float_width;

    if (values_count > 0)
    {
        if (float_width == sizeof(Float64))
            out += compressImpl<UInt64>(source, values_count, out);
        else if (float_width == sizeof(Float32))
            out += compressImpl<UInt32>(source, values_count, out);
        else
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with Wallaby codec, unsupported float width {}", static_cast<UInt32>(float_width));
    }

    if (trailing_bytes > 0)
    {
        memcpy(out, source + source_size - trailing_bytes, trailing_bytes);
        out += trailing_bytes;
    }

    return static_cast<UInt32>(out - dest);
}

UInt32 CompressionCodecWallaby::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < WALLABY_HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, source is too small");

    const UInt8 version = static_cast<UInt8>(source[0]);
    const UInt8 data_float_width = static_cast<UInt8>(source[1]);
    const UInt32 stored_size = unalignedLoadLittleEndian<UInt32>(source + 2);

    if (version != WALLABY_CODEC_VERSION)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, unsupported version {}", static_cast<UInt32>(version));
    if (stored_size != uncompressed_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, size mismatch");
    if (data_float_width != sizeof(Float64) && data_float_width != sizeof(Float32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, unsupported float width {}", static_cast<UInt32>(data_float_width));

    const char * src = source + WALLABY_HEADER_SIZE;
    const UInt32 body_size = source_size - WALLABY_HEADER_SIZE;
    const UInt32 values_count = uncompressed_size / data_float_width;
    const UInt32 trailing_bytes = uncompressed_size % data_float_width;

    UInt32 consumed = 0;
    if (values_count > 0)
    {
        if (data_float_width == sizeof(Float64))
            consumed = decompressImpl<UInt64>(src, body_size, dest, values_count);
        else
            consumed = decompressImpl<UInt32>(src, body_size, dest, values_count);
    }

    if (body_size - consumed != trailing_bytes)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, source size does not match the encoded stream");

    if (trailing_bytes > 0)
        memcpy(dest + uncompressed_size - trailing_bytes, src + consumed, trailing_bytes);

    return uncompressed_size;
}

void registerCodecWallaby(CompressionCodecFactory & factory)
{
    const auto method_code = static_cast<UInt8>(CompressionMethodByte::Wallaby);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        if (arguments && !arguments->children.empty())
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "Wallaby codec must not have parameters, given {}", arguments->children.size());

        UInt8 float_width = sizeof(Float64);
        if (column_type)
        {
            if (!WhichDataType(column_type).isNativeFloat())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Codec Wallaby is not applicable for {} because the data type is not Float*",
                    column_type->getName());

            float_width = static_cast<UInt8>(column_type->getSizeOfValueInMemory());
        }

        return std::make_shared<CompressionCodecWallaby>(float_width);
    };
    factory.registerCompressionCodecWithType("Wallaby", method_code, codec_builder);
}

CompressionCodecPtr getCompressionCodecWallaby(UInt8 float_width)
{
    return std::make_shared<CompressionCodecWallaby>(float_width);
}

}
