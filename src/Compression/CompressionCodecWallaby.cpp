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
 *   mode 1 DECIMAL_FOR:   u8 alpha, u8 bits, Int64/Int32 base (LE), u16 exception_count,
 *                         bits * 1024 / 8 bytes of FFOR-packed (q[i] - base) lanes,
 *                         exception_count * { u16 position, float_width raw value }.
 *   mode 2 DECIMAL_DELTA: u8 alpha, u8 bits, Int64/Int32 first_q (LE), u16 exception_count,
 *                         bits * 1024 / 8 bytes of FFOR-packed zigzag(q[i] - q[i-1]) lanes
 *                         (lane 0 is zero), exceptions as above.
 *   mode 3 XOR:           u32 payload byte length (LE), then a bitstream described below.
 *   mode 4 RAW:           count * float_width bytes verbatim.
 *
 * After the last vector, uncompressed_size % float_width trailing bytes are stored verbatim.
 *
 * DECIMAL modes reconstruct v[i] = Float(Float64(q[i]) / 10^alpha) with q[i] being
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

/// The cost of one stored exception: its position plus the raw value.
template <typename T>
constexpr UInt32 exceptionCost() { return sizeof(UInt16) + sizeof(T); }

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
    static constexpr UInt8 max_alpha = 18;
    static constexpr std::array<UInt8, WALLABY_LEAD_CLASSES> lead_classes{0, 8, 12, 16, 20, 24, 32, 44};
};

template <>
struct WallabyTraits<UInt32>
{
    using FloatType = Float32;
    using SignedType = Int32;
    static constexpr UInt8 width_bits = 32;
    static constexpr UInt8 trail_bits = 5;
    static constexpr UInt8 max_alpha = 10;
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

/** Quantizes a value to alpha decimal places and verifies that the division reconstructs it
  * bitwise. Returns false when the value cannot be represented this way losslessly.
  */
template <typename T>
bool quantizeValue(typename WallabyTraits<T>::FloatType value, UInt8 alpha, typename WallabyTraits<T>::SignedType & quantized)
{
    using Traits = WallabyTraits<T>;
    using SignedType = typename Traits::SignedType;
    using FloatType = typename Traits::FloatType;

    if (!std::isfinite(value))
        return false;

    const Float64 scaled = static_cast<Float64>(value) * WALLABY_POW10[alpha];
    /// Stay inside the exact llround domain.
    if (!(std::fabs(scaled) < 9.2e18))
        return false;

    const Int64 q = std::llround(scaled);
    if constexpr (std::is_same_v<SignedType, Int32>)
    {
        if (q < std::numeric_limits<Int32>::min() || q > std::numeric_limits<Int32>::max())
            return false;
    }

    const FloatType reconstructed = static_cast<FloatType>(static_cast<Float64>(q) / WALLABY_POW10[alpha]);
    if (std::bit_cast<T>(reconstructed) != std::bit_cast<T>(value))
        return false;

    quantized = static_cast<SignedType>(q);
    return true;
}

/// Returns the smallest number of decimal places that quantizes the value losslessly, if any.
template <typename T>
std::optional<UInt8> findAlpha(typename WallabyTraits<T>::FloatType value)
{
    typename WallabyTraits<T>::SignedType quantized;
    for (UInt8 alpha = 0; alpha <= WallabyTraits<T>::max_alpha; ++alpha)
        if (quantizeValue<T>(value, alpha, quantized))
            return alpha;
    return std::nullopt;
}

template <typename T>
struct DecimalEncodingResult
{
    VectorMode mode;
    UInt32 payload_size;
    UInt8 alpha;
};

/** Tries to encode a vector with one of the decimal modes into scratch.
  * Returns std::nullopt when the data does not fit the decimal representation, or when no
  * decimal encoding can come in under size_to_beat — the size of the best other encoding of
  * this vector (the RAW mode at worst, the already-measured XOR encoding when it ran first).
  */
template <typename T>
std::optional<DecimalEncodingResult<T>> encodeDecimal(
    const typename WallabyTraits<T>::FloatType * values, UInt32 count, char * scratch, UInt32 scratch_size, UInt32 size_to_beat,
    std::optional<UInt8> hint_alpha)
{
    using Traits = WallabyTraits<T>;
    using SignedType = typename Traits::SignedType;

    constexpr UInt32 header_size = 2 * sizeof(UInt8) + sizeof(SignedType) + sizeof(UInt16);

    /// Determine the number of decimal places from a sample. The sample positions follow an odd
    /// multiplicative sequence rather than a fixed stride, so that a periodic pattern of
    /// non-quantizable values (e.g. a NaN in every 32nd slot) cannot alias with the sampling
    /// and knock out the decimal modes while the full vector is within the exception budget.
    /// Sampled failures are tolerated up to a bounded fraction; the exact rejection of a
    /// candidate alpha is decided by the measured full-vector payload below.
    std::array<UInt8, 32> sampled_alphas{};
    UInt32 sampled_alpha_count = 0;
    {
        const UInt32 samples = std::min<UInt32>(count, 32);
        const UInt32 max_failures = std::max<UInt32>(3, samples / 4);
        UInt32 failures = 0;
        for (UInt32 i = 0; i < samples; ++i)
        {
            /// An odd multiplier hits every position exactly once modulo a power of two
            /// and spreads the samples uniformly for any other count.
            const UInt32 position = static_cast<UInt32>((static_cast<UInt64>(i) * 2654435761u) % count);
            if (auto sample_alpha = findAlpha<T>(values[position]))
                sampled_alphas[sampled_alpha_count++] = *sample_alpha;
            else if (++failures > max_failures)
                return std::nullopt;
        }
        if (sampled_alpha_count == 0)
            return std::nullopt;
    }

    /// Filled before use; zeroing per vector is a measurable cost on the compression path.
    alignas(64) std::array<SignedType, WALLABY_VECTOR_VALUES> quantized; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    std::array<UInt16, WALLABY_VECTOR_VALUES> exception_positions; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    UInt32 exception_count = 0;

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

    const auto quantize_all = [&](UInt8 candidate_alpha) -> bool
    {
        const UInt32 budget = exception_budget();
        exception_count = 0;
        SignedType previous_good = 0;
        Int32 first_good = -1;
        for (UInt32 i = 0; i < count; ++i)
        {
            SignedType q;
            if (quantizeValue<T>(values[i], candidate_alpha, q))
            {
                quantized[i] = q;
                previous_good = q;
                if (first_good < 0)
                    first_good = static_cast<Int32>(i);
            }
            else
            {
                if (exception_count == budget)
                    return false;
                exception_positions[exception_count] = static_cast<UInt16>(i);
                ++exception_count;
                /// Any placeholder works since the value is patched at decompression;
                /// the previous one keeps both FOR and DELTA packings narrow.
                quantized[i] = previous_good;
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
        UInt8 bits_for = 0;
        bool use_delta = false;
        SignedType base = 0;
        UInt32 payload_size = 0;
    };

    /// Chooses between Frame-of-Reference and zigzag delta packing for the quantized vector
    /// by the resulting bit width and computes the payload size of the cheaper of the two.
    const auto measure_packing = [&]() -> std::optional<Packing>
    {
        SignedType min_q = quantized[0];
        SignedType max_q = quantized[0];
        for (UInt32 i = 1; i < count; ++i)
        {
            min_q = std::min(min_q, quantized[i]);
            max_q = std::max(max_q, quantized[i]);
        }
        const T for_range = static_cast<T>(max_q) - static_cast<T>(min_q);
        const UInt8 bits_for = for_range == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(for_range));

        T max_zigzag = 0;
        bool delta_valid = true;
        for (UInt32 i = 1; i < count && delta_valid; ++i)
        {
            SignedType delta;
            if (__builtin_sub_overflow(quantized[i], quantized[i - 1], &delta))
                delta_valid = false;
            else
                max_zigzag = std::max(max_zigzag, static_cast<T>((static_cast<T>(delta) << 1) ^ static_cast<T>(delta >> (Traits::width_bits - 1))));
        }
        const UInt8 bits_delta = !delta_valid ? Traits::width_bits
            : (max_zigzag == 0 ? 0 : static_cast<UInt8>(Traits::width_bits - std::countl_zero(max_zigzag)));

        const bool use_delta = delta_valid && bits_delta < bits_for;
        const UInt8 bits = use_delta ? bits_delta : bits_for;
        if (bits >= Traits::width_bits)
            return std::nullopt;

        const UInt32 packed_bytes = Compression::FFOR::calculateBitpackedBytes(bits);
        const UInt32 payload_size = header_size + packed_bytes + exception_count * exceptionCost<T>();
        return Packing{bits, bits_for, use_delta, use_delta ? quantized[0] : min_q, payload_size};
    };

    /** Candidate scales and their evaluation. A single sampled high-precision value must not
      * force a needlessly wide scale onto the whole vector, and with mixed-precision data the
      * cheapest legal scale is often one that the 32 samples never see at all. The candidate
      * set is therefore grown from three sources, each of which costs almost nothing on top
      * of work already done:
      *   - every distinct sampled alpha;
      *   - the trailing-decimal-zero counts of the quantized values at the reference alpha A:
      *     a value whose q ends in k decimal zeros is also exactly representable at A - k, so
      *     the distinct values of A - k enumerate the scales at which some subset of the vector
      *     becomes exactly representable — including scales sampling missed (the property can
      *     fail near the precision limit, but a wrong candidate merely measures worse below);
      *   - the minimal alphas of a few exception values of each evaluated candidate: when the
      *     samples only saw a low-precision majority, this discovers the higher scale that
      *     would absorb a high-precision minority into the packed lanes.
      * Every candidate is evaluated against the full vector and the smallest measured payload
      * wins; values that do not quantize with the winning alpha become exceptions. The most
      * frequent sampled alpha is evaluated first: it is the best single guess for the data's
      * true scale, so the pruning estimates below start from a tight baseline and the common
      * case (one homogeneous scale) stays at a single full-vector pass — in particular, a rare
      * sampled high-precision outlier no longer costs a wasted pass at its needlessly wide
      * scale before the majority scale wins.
      */
    UInt8 alpha = 0;
    /// The alpha whose quantization the scratch arrays currently hold; a failed quantize_all
    /// leaves them clobbered halfway, so it resets this tracker.
    std::optional<UInt8> scratch_alpha;
    std::optional<Packing> best;

    bool considered[Traits::max_alpha + 1] = {};
    bool evaluated[Traits::max_alpha + 1] = {};
    UInt32 sampled_frequency[Traits::max_alpha + 1] = {};
    for (UInt32 i = 0; i < sampled_alpha_count; ++i)
    {
        considered[sampled_alphas[i]] = true;
        ++sampled_frequency[sampled_alphas[i]];
    }
    UInt8 mode_alpha = 0;
    for (UInt8 a = 1; a <= Traits::max_alpha; ++a)
        if (sampled_frequency[a] > sampled_frequency[mode_alpha])
            mode_alpha = a;

    /// The first successfully evaluated alpha (normally the most frequent sampled one) and its
    /// measured packed widths; the pruning estimates scale the widths from there.
    UInt8 reference_alpha = 0;
    bool reference_filled = false;
    std::optional<UInt8> reference_packed_bits;
    std::optional<UInt8> reference_for_bits;

    /** Cheap lower-bound estimate deciding whether a candidate scale can still beat the best
      * measured payload. Exceptions are estimated from the sampled minimal alphas: a sample
      * that needs more decimal places than the candidate offers becomes an exception. The count
      * is extrapolated from the sample with a two-failure allowance subtracted, so sampling
      * variance biases the estimate down and a viable candidate is never skipped merely because
      * the sample overrepresented high-precision values. (The trailing-zero structure of the
      * quantized values is deliberately not used here: for Float32 promoted to Float64 the
      * quantized integers at scales beyond the type's decimal precision carry junk low digits,
      * which would wildly overestimate the exception count.) The packed width scales by
      * log2(10) bits per scale step, always rounded in the candidate's favor: downward from the
      * cheaper of the reference packings, upward from its Frame-of-Reference width (the range
      * of the quantized values grows by exactly a factor of ten per step, while no such bound
      * holds for the delta packing, whose exception placeholders differ between scales), with
      * two extra bits of slack for values entering the lanes.
      */
    const auto estimate_allows = [&](UInt8 candidate) -> bool
    {
        if (!reference_filled || candidate == reference_alpha)
            return true;
        UInt32 sampled_failures = 0;
        for (UInt32 i = 0; i < sampled_alpha_count; ++i)
            sampled_failures += sampled_alphas[i] > candidate ? 1 : 0;
        const UInt32 discounted_failures = sampled_failures > 2 ? sampled_failures - 2 : 0;
        const UInt32 exceptions_estimate = static_cast<UInt32>(
            static_cast<UInt64>(discounted_failures) * count / std::max<UInt32>(sampled_alpha_count, 1));
        UInt32 bits_estimate = 0;
        if (candidate < reference_alpha)
        {
            if (!reference_packed_bits)
                return true;
            const UInt32 bits_shrink = static_cast<UInt32>(reference_alpha - candidate) * 3322 / 1000;
            bits_estimate = reference_packed_bits.value() > bits_shrink ? reference_packed_bits.value() - bits_shrink : 0;
        }
        else
        {
            if (!reference_for_bits)
                return true;
            const UInt32 bits_grow = static_cast<UInt32>(candidate - reference_alpha) * 3322 / 1000;
            bits_estimate = reference_for_bits.value() + bits_grow;
            bits_estimate = bits_estimate > 2 ? bits_estimate - 2 : 0;
            if (bits_estimate >= Traits::width_bits)
                return false;
        }
        const UInt32 size_estimate = header_size + Compression::FFOR::calculateBitpackedBytes(static_cast<UInt8>(bits_estimate))
            + exceptions_estimate * exceptionCost<T>();
        /// best_total_size also carries the caller-provided bound, so a candidate that cannot
        /// beat the other encoding of this vector is skipped even before any candidate wins.
        return size_estimate < best_total_size;
    };

    bool candidates_pending = false;

    const auto evaluate_candidate = [&](UInt8 candidate)
    {
        evaluated[candidate] = true;

        if (!estimate_allows(candidate))
            return;

        if (!quantize_all(candidate))
        {
            scratch_alpha.reset();
            return;
        }
        scratch_alpha = candidate;

        if (!reference_filled)
        {
            reference_filled = true;
            reference_alpha = candidate;
            /// Candidate generation from trailing decimal zeros: a value whose quantized
            /// integer ends in k decimal zeros is also exactly representable at
            /// candidate - k, so those scales are worth measuring even when the samples
            /// never saw them. (This can miss structure for Float32 at scales beyond the
            /// type's decimal precision, where the quantized integers carry junk low
            /// digits; the sampled alphas and the exception probes below still stand.)
            for (UInt32 i = 0; i < count; ++i)
            {
                SignedType q = quantized[i];
                UInt32 trailing_zeros = 0;
                if (q == 0)
                    trailing_zeros = candidate;
                else
                    while (q % 10 == 0 && trailing_zeros < candidate)
                    {
                        q /= 10;
                        ++trailing_zeros;
                    }
                considered[candidate - trailing_zeros] = true;
            }
        }

        /// Probe a few exception values for the scale they would need: when sampling only
        /// saw a low-precision majority, this is what discovers the higher-precision scale.
        const UInt32 probe_limit = std::min<UInt32>(exception_count, 8);
        for (UInt32 e = 0; e < probe_limit; ++e)
        {
            if (auto exception_alpha = findAlpha<T>(values[exception_positions[e]]))
            {
                if (!considered[*exception_alpha])
                {
                    considered[*exception_alpha] = true;
                    candidates_pending = true;
                }
            }
        }

        const auto packing = measure_packing();
        if (packing && candidate == reference_alpha)
        {
            reference_packed_bits = packing->bits;
            reference_for_bits = packing->bits_for;
        }
        if (packing && (!best || packing->payload_size < best->payload_size))
        {
            best = *packing;
            alpha = candidate;
            best_total_size = std::min(best_total_size, packing->payload_size);
        }
    };

    /// Consecutive vectors of a column almost always share their decimal scale, so the scale
    /// that won the previous vector is the best first guess: evaluating it first makes it the
    /// reference and lets the estimates prune most other candidates without a full pass. The
    /// most frequent sampled alpha is the fallback guess (and the second candidate on vectors
    /// where the data changed); a stale hint costs at most one extra full-vector pass.
    if (hint_alpha && *hint_alpha <= Traits::max_alpha)
    {
        considered[*hint_alpha] = true;
        evaluate_candidate(*hint_alpha);
    }
    if (!evaluated[mode_alpha])
        evaluate_candidate(mode_alpha);

    /// Fixed-point loop: evaluating a candidate can add new candidates (from exception probing),
    /// so sweep the domain until no unevaluated candidate remains. The domain has at most
    /// max_alpha + 1 scales, and each is evaluated at most once.
    candidates_pending = true;
    while (candidates_pending)
    {
        candidates_pending = false;
        for (Int32 candidate_signed = Traits::max_alpha; candidate_signed >= 0; --candidate_signed)
        {
            const UInt8 candidate = static_cast<UInt8>(candidate_signed);
            if (!considered[candidate] || evaluated[candidate])
                continue;
            evaluate_candidate(candidate);
        }
    }
    if (!best)
        return std::nullopt;

    /// The quantization scratch may hold a later evaluated candidate; recompute the winner.
    if (scratch_alpha != alpha && !quantize_all(alpha))
        return std::nullopt;

    const UInt8 bits = best->bits;
    const bool use_delta = best->use_delta;

    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> lanes; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> packed; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)

    if (use_delta)
    {
        lanes[0] = 0;
        for (UInt32 i = 1; i < count; ++i)
        {
            const SignedType delta = quantized[i] - quantized[i - 1];
            lanes[i] = (static_cast<T>(delta) << 1) ^ static_cast<T>(delta >> (Traits::width_bits - 1));
        }
        for (UInt32 i = count; i < WALLABY_VECTOR_VALUES; ++i)
            lanes[i] = 0;
        Compression::FFOR::bitPack(lanes.data(), packed.data(), bits, T{0});
    }
    else
    {
        for (UInt32 i = 0; i < count; ++i)
            lanes[i] = static_cast<T>(quantized[i]);
        for (UInt32 i = count; i < WALLABY_VECTOR_VALUES; ++i)
            lanes[i] = static_cast<T>(best->base);
        Compression::FFOR::bitPack(lanes.data(), packed.data(), bits, static_cast<T>(best->base));
    }

    const UInt32 packed_bytes = Compression::FFOR::calculateBitpackedBytes(bits);
    const UInt32 payload_size = best->payload_size;
    if (payload_size > scratch_size)
        return std::nullopt;

    char * out = scratch;
    *out++ = static_cast<char>(alpha);
    *out++ = static_cast<char>(bits);
    unalignedStoreLittleEndian<SignedType>(out, best->base);
    out += sizeof(SignedType);
    unalignedStoreLittleEndian<UInt16>(out, static_cast<UInt16>(exception_count));
    out += sizeof(UInt16);
    memcpy(out, packed.data(), packed_bytes);
    out += packed_bytes;
    for (UInt32 i = 0; i < exception_count; ++i)
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

        const UInt32 candidates[3] = {newest_slot, probe[highBitsProbeKey(value)], low_probe[lowBitsProbeKey(value)]};
        for (UInt32 candidate = 0; candidate < 3 && best_branch != Branch::Equal; ++candidate)
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
void decodeXor(const char * payload, UInt32 payload_size, T * out, UInt32 count)
{
    using Traits = WallabyTraits<T>;
    constexpr UInt8 width = Traits::width_bits;

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
    out[0] = previous;
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
                out[produced + j] = previous;
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

        out[produced] = value;
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
    std::optional<UInt8> previous_alpha;

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
    alignas(64) std::array<T, WALLABY_VECTOR_VALUES> words{};

    char * out = dest;
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
                    words[i] = word;
                break;
            }
            case VectorMode::DecimalFor:
            case VectorMode::DecimalDelta:
            {
                require(2 * sizeof(UInt8) + sizeof(SignedType) + sizeof(UInt16));
                const UInt8 alpha = static_cast<UInt8>(*src++);
                const UInt8 bits = static_cast<UInt8>(*src++);
                const SignedType base = unalignedLoadLittleEndian<SignedType>(src);
                src += sizeof(SignedType);
                const UInt16 exception_count = unalignedLoadLittleEndian<UInt16>(src);
                src += sizeof(UInt16);

                if (alpha > Traits::max_alpha || bits >= Traits::width_bits || exception_count > count)
                    throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Wallaby-encoded data, corrupt decimal header");

                const UInt32 packed_bytes = Compression::FFOR::calculateBitpackedBytes(bits);
                require(packed_bytes);
                memcpy(lanes.data(), src, packed_bytes);
                src += packed_bytes;

                if (mode == VectorMode::DecimalFor)
                {
                    Compression::FFOR::bitUnpack(lanes.data(), unpacked.data(), bits, static_cast<T>(base));
                    for (UInt32 i = 0; i < count; ++i)
                    {
                        const auto q = static_cast<SignedType>(unpacked[i]);
                        words[i] = std::bit_cast<T>(static_cast<typename Traits::FloatType>(static_cast<Float64>(q) / WALLABY_POW10[alpha]));
                    }
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
                        const auto q = static_cast<SignedType>(accumulator);
                        words[i] = std::bit_cast<T>(static_cast<typename Traits::FloatType>(static_cast<Float64>(q) / WALLABY_POW10[alpha]));
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
                    words[position] = raw;
                }
                break;
            }
            case VectorMode::Xor:
            {
                require(sizeof(UInt32));
                const UInt32 payload_size = unalignedLoadLittleEndian<UInt32>(src);
                src += sizeof(UInt32);
                require(payload_size);
                decodeXor<T>(src, payload_size, words.data(), count);
                src += payload_size;
                break;
            }
            case VectorMode::Raw:
            {
                require(static_cast<size_t>(count) * sizeof(T));
                memcpy(words.data(), src, static_cast<size_t>(count) * sizeof(T));
                src += static_cast<size_t>(count) * sizeof(T);
                break;
            }
        }

        memcpy(out, words.data(), static_cast<size_t>(count) * sizeof(T));
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
