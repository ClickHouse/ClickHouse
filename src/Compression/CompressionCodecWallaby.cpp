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
    alignas(64) std::array<SignedType, WALLABY_VECTOR_VALUES> quantized; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> adjustments; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    std::array<UInt16, WALLABY_VECTOR_VALUES> exception_positions; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    std::array<bool, WALLABY_VECTOR_VALUES> is_quantization_exception; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
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

    /// allow_floor_abort is off for the scan that seeds the reference quantization (the
    /// trailing-zero candidate generation needs the full vector) and both aborts are off for
    /// the winner's re-quantization (its comparison price may sit below its own floor and its
    /// exceptions may exceed a budget tightened by that price).
    const auto quantize_all = [&](Int32 candidate_alpha, bool allow_budget_abort = true, bool allow_floor_abort = true) -> bool
    {
        const UInt32 budget = allow_budget_abort ? exception_budget() : count;
        exception_count = 0;
        soft_exception_count = 0;
        max_adjustment_zigzag = 0;
        monotone_exceptions = 0;
        std::fill(is_quantization_exception.begin(), is_quantization_exception.begin() + count, false);
        SignedType previous_good = 0;
        Int32 first_good = -1;
        /// Running bounds for the periodic floor check below: the Frame-of-Reference range and
        /// the largest zigzag delta only grow as the scan proceeds, so the packed-lane bytes
        /// they imply are a provable lower bound on any packing of this scale at every point.
        SignedType running_min = 0;
        SignedType running_max = 0;
        T max_delta_zigzag = 0;
        bool delta_overflow = false;
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
                if (first_good < 0)
                {
                    running_min = q;
                    running_max = q;
                }
                else
                {
                    running_min = std::min(running_min, q);
                    running_max = std::max(running_max, q);
                    SignedType delta;
                    if (__builtin_sub_overflow(q, previous_good, &delta))
                        delta_overflow = true;
                    else
                        max_delta_zigzag = std::max(
                            max_delta_zigzag, (static_cast<T>(delta) << 1) ^ static_cast<T>(delta >> (Traits::width_bits - 1)));
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
            /// Abandon a scale whose lanes alone already outgrow the best encoding known for
            /// this vector: the cheaper of the running Frame-of-Reference and delta widths is a
            /// provable floor (near-misses cost extra on top, exceptions are counted as seen).
            /// This is what keeps a probe of a very fine scale over wide-spread data from
            /// scanning the whole vector; a scale that collapses the lanes keeps them narrow
            /// from the start and is never touched.
            if (allow_floor_abort && (i & 127u) == 127u && first_good >= 0)
            {
                const T for_range = static_cast<T>(running_max) - static_cast<T>(running_min);
                const UInt8 for_bits = for_range == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(for_range));
                const UInt8 delta_bits = delta_overflow ? Traits::width_bits
                    : (max_delta_zigzag == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(max_delta_zigzag)));
                const UInt32 floor_bytes = header_size
                    + Compression::FFOR::calculateBitpackedBytes(std::min(for_bits, delta_bits))
                    + exception_count * exceptionCost<T>();
                if (floor_bytes >= best_total_size)
                    return false;
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
    const auto walk_delta = [&](UInt8 cap_bits, T * delta_lanes, UInt16 * exiled_positions) -> UInt32
    {
        SignedType chain = quantized[0];
        UInt32 exceptions = 0;
        if (is_quantization_exception[0])
        {
            if (exiled_positions)
                exiled_positions[exceptions] = 0;
            ++exceptions;
        }
        if (delta_lanes)
            delta_lanes[0] = 0;
        for (UInt32 i = 1; i < count; ++i)
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
      * lanes: a histogram of per-value widths gives the optimal cap in closed form for the
      * Frame-of-Reference offsets and for the adjustments (per-position independence makes them
      * exact), and proposes a candidate cap for the deltas that an exact chain walk verifies,
      * since an exiled delta partially reappears at the next position. The Frame-of-Reference
      * base stays at the vector minimum, so only large values are exiled — a single small
      * outlier still widens the lanes.
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
        UInt32 delta_width_histogram[Traits::width_bits + 1] = {};
        T max_zigzag = 0;
        bool delta_valid = true;
        if (allow_capping)
        {
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
                        ++delta_width_histogram[zigzag == 0 ? 0 : Traits::width_bits - std::countl_zero(zigzag)];
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

        std::optional<Packing> best_packing;
        if (bits_for_full < Traits::width_bits)
        {
            if (!allow_capping)
            {
                const UInt32 total = header_size + lanes_bytes(bits_for_full)
                    + (exception_count + soft_exception_count) * exceptionCost<T>();
                best_packing = Packing{bits_for_full, 0, false, min_q, total};
            }
            else
            {
                /// FOR: the histograms are exact — every value above a cap is one exception,
                /// independently of the others — so scan all cap widths in closed form, then
                /// plan the adjustment lanes over the surviving positions in one more pass.
                UInt32 outliers = 0;
                UInt8 best_w = bits_for_full;
                UInt32 best_cost = header_size + lanes_bytes(bits_for_full) + exception_count * exceptionCost<T>();
                for (Int32 w = bits_for_full - 1; w >= 0; --w)
                {
                    outliers += for_width_histogram[w + 1];
                    const UInt32 cost = header_size + lanes_bytes(static_cast<UInt8>(w))
                        + (exception_count + outliers) * exceptionCost<T>();
                    if (cost < best_cost)
                    {
                        best_cost = cost;
                        best_w = static_cast<UInt8>(w);
                    }
                }
                UInt32 for_exiles = 0;
                UInt32 adjustment_histogram[Traits::width_bits + 1] = {};
                UInt8 adjustment_full_bits = 0;
                for (UInt32 i = 0; i < count; ++i)
                {
                    const T offset = static_cast<T>(quantized[i]) - static_cast<T>(min_q);
                    const bool exiled = is_quantization_exception[i]
                        || (best_w < Traits::width_bits && offset >= (T{1} << best_w));
                    exile_scratch[i] = exiled;
                    if (exiled)
                    {
                        for_exiles += !is_quantization_exception[i] ? 1 : 0;
                        continue;
                    }
                    if (max_adjustment_zigzag != 0)
                    {
                        const T adjustment = adjustments[i];
                        const UInt8 w = adjustment == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(adjustment));
                        ++adjustment_histogram[w];
                        adjustment_full_bits = std::max(adjustment_full_bits, w);
                    }
                }
                UInt8 adjustment_bits = adjustment_full_bits;
                UInt32 adjustment_cost = lanes_bytes(adjustment_full_bits);
                {
                    UInt32 adjustment_outliers = 0;
                    UInt32 running_cost = adjustment_cost;
                    for (Int32 w = adjustment_full_bits - 1; w >= 0; --w)
                    {
                        adjustment_outliers += adjustment_histogram[w + 1];
                        const UInt32 cost
                            = lanes_bytes(static_cast<UInt8>(w)) + adjustment_outliers * exceptionCost<T>();
                        if (cost < running_cost)
                        {
                            running_cost = cost;
                            adjustment_bits = static_cast<UInt8>(w);
                            adjustment_cost = cost;
                        }
                    }
                }
                const UInt32 total = header_size + lanes_bytes(best_w) + adjustment_cost
                    + (exception_count + for_exiles) * exceptionCost<T>();
                best_packing = Packing{best_w, adjustment_bits, false, min_q, total};
            }
        }

        if (delta_valid && bits_delta_full < Traits::width_bits)
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
                if (w >= bits_delta_full)
                {
                    for (UInt32 i = 0; i < count; ++i)
                        exile_scratch[i] = is_quantization_exception[i];
                    walked_exceptions = exception_count;
                }
                else
                {
                    UInt16 exiled[WALLABY_VECTOR_VALUES]; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
                    walked_exceptions = walk_delta(w, nullptr, exiled);
                    std::fill(exile_scratch.begin(), exile_scratch.begin() + count, false);
                    for (UInt32 e = 0; e < walked_exceptions; ++e)
                        exile_scratch[exiled[e]] = true;
                }
                UInt8 adjustment_bits = 0;
                UInt32 adjustment_cost = 0;
                UInt32 adjustment_exiles = 0;
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
                    adjustment_bits = adjustment_full_bits;
                    adjustment_cost = lanes_bytes(adjustment_full_bits);
                    UInt32 adjustment_outliers = 0;
                    UInt32 running_cost = adjustment_cost;
                    for (Int32 aw = adjustment_full_bits - 1; aw >= 0; --aw)
                    {
                        adjustment_outliers += adjustment_histogram[aw + 1];
                        const UInt32 cost
                            = lanes_bytes(static_cast<UInt8>(aw)) + adjustment_outliers * exceptionCost<T>();
                        if (cost < running_cost)
                        {
                            running_cost = cost;
                            adjustment_bits = static_cast<UInt8>(aw);
                            adjustment_cost = lanes_bytes(static_cast<UInt8>(aw));
                            adjustment_exiles = adjustment_outliers;
                        }
                    }
                }
                const UInt32 total = header_size + lanes_bytes(w) + adjustment_cost
                    + (walked_exceptions + adjustment_exiles) * exceptionCost<T>();
                return Packing{w, adjustment_bits, true, quantized[0], total};
            };

            const Packing uncapped = evaluate_delta(bits_delta_full);
            if (!best_packing || uncapped.payload_size < best_packing->payload_size)
                best_packing = uncapped;

            if (allow_capping)
            {
                UInt32 outliers = 0;
                UInt8 estimated_w = bits_delta_full;
                UInt32 estimated_cost = uncapped.payload_size;
                for (Int32 w = bits_delta_full - 1; w >= 0; --w)
                {
                    outliers += delta_width_histogram[w + 1];
                    const UInt32 cost = header_size + lanes_bytes(static_cast<UInt8>(w))
                        + (exception_count + outliers) * exceptionCost<T>();
                    if (cost < estimated_cost)
                    {
                        estimated_cost = cost;
                        estimated_w = static_cast<UInt8>(w);
                    }
                }
                if (estimated_w < bits_delta_full && estimated_cost + 64 < best_packing->payload_size)
                {
                    const Packing capped = evaluate_delta(estimated_w);
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
    /// The alpha whose quantization the scratch arrays currently hold; a failed quantize_all
    /// leaves them clobbered halfway, so it resets this tracker.
    std::optional<Int32> scratch_alpha;
    std::optional<Packing> best;
    /// The comparison price of the best packing: its recorded (realizable) size minus the
    /// discount for near-misses that adjustment lanes would absorb more cheaply than the
    /// exception list the uncapped measure prices them as.
    UInt32 best_price = 0;

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
        while (header_size + bound_bytes(exceptions_lower_bound) < best_total_size)
        {
            if (!sample_can_grow || sampled_positions >= WALLABY_MAX_SAMPLES)
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

    const auto evaluate_candidate = [&](Int32 candidate)
    {
        evaluated[alpha_index(candidate)] = true;

        if (!estimate_allows(candidate))
            return;

        const bool quantized_all = quantize_all(candidate, true, reference_filled);
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
            scratch_alpha.reset();
            return;
        }
        scratch_alpha = candidate;

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

        /// Probe exception values for the scale they would need: when sampling only saw a
        /// low-precision majority, this is what discovers the higher-precision scale. The probes
        /// are spread over the whole exception list with a stride instead of taking the first
        /// few, so that a handful of very wide outliers at the front of the vector cannot hide
        /// the scale that the remaining exceptions share.
        if (exception_count > 0)
        {
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
        }

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

        const auto packing = measure_packing(false);
        if (!packing)
            return;
        /// The uncapped measure prices every near-miss as an exception, which is realizable but
        /// overprices a soft-heavy scale against one that is exact everywhere: full-width
        /// adjustment lanes are also inside the capped optimizer's search space, and their cost
        /// bounds the winner's final size just as well. Candidates compete on the cheaper of the
        /// two plans; the recorded packing keeps the realizable exception-list size.
        const UInt32 adjustment_lanes_bytes = max_adjustment_zigzag == 0 ? 0
            : Compression::FFOR::calculateBitpackedBytes(
                static_cast<UInt8>(Traits::width_bits - std::countl_zero(max_adjustment_zigzag)));
        const UInt32 soft_bytes = soft_exception_count * exceptionCost<T>();
        const UInt32 soft_discount = soft_bytes - std::min(soft_bytes, adjustment_lanes_bytes);
        const UInt32 price = packing->payload_size - std::min(packing->payload_size, soft_discount);
        if (!best || price < best_price)
        {
            best = *packing;
            best_price = price;
            alpha = candidate;
            best_total_size = std::min(best_total_size, price);
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

    /// The quantization scratch may hold a later evaluated candidate; recompute the winner.
    if (scratch_alpha != alpha && !quantize_all(alpha, false, false))
        return std::nullopt;

    /// The capping analysis (PFOR-style patching of lane-width outliers into exceptions, and
    /// the reverse conversion of near-miss exceptions into adjustment lanes) runs once per
    /// vector, on the winning scale only. It is skipped when even a large gain could not bring
    /// the decimal encoding under the best other encoding of this vector: the near-misses may
    /// become almost free, the rest recovers more than a quarter only in contrived cases.
    const UInt32 soft_exception_bytes = soft_exception_count * exceptionCost<T>();
    const UInt32 payload_beyond_soft = best->payload_size - std::min(best->payload_size, soft_exception_bytes);
    /// A winner whose comparison price was discounted must take the capped pass: the recorded
    /// packing still stores its near-misses as exceptions, and only this pass makes the cheaper
    /// adjustment-lane plan real (it always finds one at least as cheap as the price).
    if (payload_beyond_soft * 4 < best_total_size * 5 || best_price < best->payload_size)
        if (const auto capped = measure_packing(true); capped && capped->payload_size < best->payload_size)
            best = *capped;

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
        const UInt32 walked = walk_delta(bits, lanes.data(), exiled);
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
    const bool omit_trail = (reader.readBits(8) & 1) != 0;

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
        }
        else
        {
            *out++ = static_cast<char>(VectorMode::Raw);
            memcpy(out, words.data(), raw_size);
            out += raw_size;
            previous_winner = VectorMode::Raw;
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
                    || adjustment_bits >= Traits::width_bits || exception_count > count)
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
                if (mode == VectorMode::DecimalFor)
                {
                    Compression::FFOR::bitUnpack(lanes.data(), unpacked.data(), bits, static_cast<T>(base));
                    if (adjustment_bits == 0)
                        for (UInt32 i = 0; i < count; ++i)
                            emit(i, reconstruct(static_cast<SignedType>(unpacked[i])));
                    else
                        for (UInt32 i = 0; i < count; ++i)
                            emit(i, adjust(i, reconstruct(static_cast<SignedType>(unpacked[i]))));
                }
                else
                {
                    Compression::FFOR::bitUnpack(lanes.data(), unpacked.data(), bits, T{0});
                    T accumulator = static_cast<T>(base);
                    for (UInt32 i = 0; i < count; ++i)
                    {
                        if (i > 0)
                        {
                            const T zigzag = unpacked[i];
                            accumulator += (zigzag >> 1) ^ (T{0} - (zigzag & T{1}));
                        }
                        const T reconstructed = reconstruct(static_cast<SignedType>(accumulator));
                        emit(i, adjustment_bits == 0 ? reconstructed : adjust(i, reconstructed));
                    }
                }

                require(exception_count * (sizeof(UInt16) + sizeof(T)));
                for (UInt32 i = 0; i < exception_count; ++i)
                {
                    const UInt16 position = unalignedLoadLittleEndian<UInt16>(src);
                    src += sizeof(UInt16);
                    const T raw = unalignedLoadLittleEndian<T>(src);
                    src += sizeof(T);
                    if (position >= count)
                        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, corrupt exception position");
                    emit(position, raw);
                }
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
