#include <Processors/Formats/Impl/Parquet/ALPEncoding.h>

#include <base/unaligned.h>
#include <Common/PODArray.h>

#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <arrow/util/bpacking_internal.h>

#include <algorithm>
#include <bit>
#include <climits>
#include <limits>
#include <optional>
#include <tuple>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace DB::Parquet
{

namespace
{

constexpr size_t ALP_PAGE_HEADER_SIZE = 7;
constexpr UInt8 ALP_COMPRESSION_MODE = 0;
constexpr UInt8 ALP_INTEGER_ENCODING_FOR_BIT_PACKING = 0;
constexpr UInt8 ALP_MIN_LOG_VECTOR_SIZE = 3;
constexpr UInt8 ALP_MAX_LOG_VECTOR_SIZE = 15;

/// The encoder writes vectors of the size recommended by the specification.
constexpr UInt8 ALP_ENCODER_LOG_VECTOR_SIZE = 10;
constexpr size_t ALP_ENCODER_VECTOR_SIZE = 1uz << ALP_ENCODER_LOG_VECTOR_SIZE;

/// The decoding is normative: every reader computes `encoded * 10^f * 10^-e` with exactly these
/// constants, the correctly rounded values of the decimal literals (not results of `pow`), in the
/// precision of the value type. That's why there are separate tables for `float` and `double`
/// (e.g. `0.1f` is not `float(0.1)` rounded twice, it's the `float` nearest to 1/10).
/// Unlike `CompressionCodecALP`, `FLOAT` values are not processed in `Float64`: the result would
/// not be what the other readers decode.
constexpr float POW10_FLOAT[] = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f};
constexpr float NEG_POW10_FLOAT[] = {1e0f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f};
constexpr double POW10_DOUBLE[] = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18};
constexpr double NEG_POW10_DOUBLE[] = {
    1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16, 1e-17, 1e-18};

template <typename T>
struct ALPTraits;

template <>
struct ALPTraits<float>
{
    using Exact = UInt32;
    using Signed = Int32;
    static constexpr UInt8 max_exponent = 10;
    static constexpr const float * pow10 = POW10_FLOAT;
    static constexpr const float * neg_pow10 = NEG_POW10_FLOAT;
    /// 2^22 + 2^23: adding and subtracting it rounds to the nearest integer (ties to even) any
    /// value whose magnitude is below 2^22, positive or negative.
    static constexpr float magic = 12582912.0f;
    /// Bounds (exactly representable) of the values that convert to `Signed` without overflow.
    static constexpr float min_encodable = -2147483648.0f;
    static constexpr float max_encodable_exclusive = 2147483648.0f;
};

template <>
struct ALPTraits<double>
{
    using Exact = UInt64;
    using Signed = Int64;
    static constexpr UInt8 max_exponent = 18;
    static constexpr const double * pow10 = POW10_DOUBLE;
    static constexpr const double * neg_pow10 = NEG_POW10_DOUBLE;
    /// 2^51 + 2^52, see above.
    static constexpr double magic = 6755399441055744.0;
    static constexpr double min_encodable = -9223372036854775808.0;
    static constexpr double max_encodable_exclusive = 9223372036854775808.0;
};

static_assert(std::size(POW10_FLOAT) == ALPTraits<float>::max_exponent + 1);
static_assert(std::size(NEG_POW10_FLOAT) == ALPTraits<float>::max_exponent + 1);
static_assert(std::size(POW10_DOUBLE) == ALPTraits<double>::max_exponent + 1);
static_assert(std::size(NEG_POW10_DOUBLE) == ALPTraits<double>::max_exponent + 1);

/// `AlpInfo` (exponent, factor, num_exceptions) + `ForInfo` (frame_of_reference, bit_width).
template <typename T>
constexpr size_t vectorHeaderSize()
{
    return 4 + sizeof(T) + 1;
}


/// Reading.

/// An `ALP` page with a validated page header and offset sequence. The contents of each vector
/// are validated when it is decoded.
template <typename T>
struct ALPPage
{
    using Traits = ALPTraits<T>;
    using Exact = typename Traits::Exact;
    using Signed = typename Traits::Signed;

    size_t num_elements = 0;
    UInt8 log_vector_size = 0;
    size_t vector_size = 0;
    size_t num_vectors = 0;
    /// The offset array. The offsets are relative to its start, and so is everything else below.
    const char * body = nullptr;
    size_t body_size = 0;

    explicit ALPPage(std::span<const char> data)
    {
        if (data.size() < ALP_PAGE_HEADER_SIZE)
            throw Exception(ErrorCodes::INCORRECT_DATA, "ALP page is too short: {} bytes", data.size());

        const unsigned compression_mode = static_cast<UInt8>(data[0]);
        const unsigned integer_encoding = static_cast<UInt8>(data[1]);
        log_vector_size = static_cast<UInt8>(data[2]);
        const Int32 num_elements_signed = unalignedLoadLittleEndian<Int32>(data.data() + 3);

        if (compression_mode != ALP_COMPRESSION_MODE)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported ALP compression mode: {}", compression_mode);
        if (integer_encoding != ALP_INTEGER_ENCODING_FOR_BIT_PACKING)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported ALP integer encoding: {}", integer_encoding);
        if (log_vector_size < ALP_MIN_LOG_VECTOR_SIZE || log_vector_size > ALP_MAX_LOG_VECTOR_SIZE)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid ALP log_vector_size: {}", static_cast<unsigned>(log_vector_size));
        if (num_elements_signed < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid ALP number of elements: {}", num_elements_signed);

        num_elements = static_cast<size_t>(num_elements_signed);
        vector_size = 1uz << log_vector_size;
        num_vectors = (num_elements + vector_size - 1) >> log_vector_size;
        body = data.data() + ALP_PAGE_HEADER_SIZE;
        body_size = data.size() - ALP_PAGE_HEADER_SIZE;

        if (body_size / 4 < num_vectors)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ALP page is too short for the offsets of its {} vectors: {} bytes", num_vectors, data.size());

        /// The vectors are stored back to back right after the offset array, and the last one ends
        /// exactly at the end of the page. Validate the whole offset sequence here, by reading only
        /// the vector headers, so that it holds even for vectors that are skipped and never decoded.
        size_t expected_offset = num_vectors * 4;
        for (size_t vector_idx = 0; vector_idx < num_vectors; ++vector_idx)
        {
            const size_t offset = unalignedLoadLittleEndian<UInt32>(body + vector_idx * 4);
            if (offset != expected_offset)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Invalid offset of ALP vector {}: {}, expected {}", vector_idx, offset, expected_offset);
            if (body_size - offset < vectorHeaderSize<T>())
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ALP page is too short for the header of vector {}: the page body has {} bytes", vector_idx, body_size);
            const char * ptr = body + offset;
            const size_t n = vectorLength(vector_idx);
            const size_t num_exceptions = unalignedLoadLittleEndian<UInt16>(ptr + 2);
            const unsigned bit_width = static_cast<UInt8>(ptr[4 + sizeof(T)]);
            if (bit_width > sizeof(T) * 8)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid ALP bit width in vector {}: {}", vector_idx, bit_width);
            if (num_exceptions > n)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ALP vector {} has {} exceptions, but only {} values", vector_idx, num_exceptions, n);
            expected_offset = offset + vectorHeaderSize<T>() + (n * bit_width + 7) / 8 + num_exceptions * (sizeof(UInt16) + sizeof(T));
            if (expected_offset > body_size)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ALP vector {} needs {} bytes, but the page has only {} bytes left",
                    vector_idx, expected_offset - offset, body_size - offset);
        }
        if (expected_offset != body_size)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ALP page has {} bytes after its last vector", body_size - expected_offset);
    }

    size_t vectorLength(size_t vector_idx) const
    {
        chassert(vector_idx < num_vectors);
        return std::min(vector_size, num_elements - (vector_idx << log_vector_size));
    }

    /// Writes the `vectorLength(vector_idx)` values of the vector to `out`.
    /// `deltas` must have room for as many values.
    void decodeVector(size_t vector_idx, T * __restrict out, Exact * __restrict deltas) const
    {
        const size_t n = vectorLength(vector_idx);

        const size_t offset = unalignedLoadLittleEndian<UInt32>(body + vector_idx * 4);
        if (offset < num_vectors * 4 || offset > body_size || body_size - offset < vectorHeaderSize<T>())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Invalid offset of ALP vector {}: {}, the page body has {} bytes", vector_idx, offset, body_size);

        const char * ptr = body + offset;
        const size_t available = body_size - offset - vectorHeaderSize<T>();

        const unsigned exponent = static_cast<UInt8>(ptr[0]);
        const unsigned factor = static_cast<UInt8>(ptr[1]);
        const size_t num_exceptions = unalignedLoadLittleEndian<UInt16>(ptr + 2);
        const Exact frame_of_reference = unalignedLoadLittleEndian<Exact>(ptr + 4);
        const unsigned bit_width = static_cast<UInt8>(ptr[4 + sizeof(T)]);
        ptr += vectorHeaderSize<T>();

        if (exponent > Traits::max_exponent || factor > exponent)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Invalid ALP exponent and factor in vector {}: {} and {}", vector_idx, exponent, factor);
        /// The bit width, the number of exceptions and the size of the vector were checked in the constructor.
        chassert(bit_width <= sizeof(T) * 8 && num_exceptions <= n);

        const size_t packed_size = (n * bit_width + 7) / 8;
        const size_t exceptions_size = num_exceptions * (sizeof(UInt16) + sizeof(T));
        if (available < packed_size + exceptions_size)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ALP vector {} needs {} bytes, but the page has only {} bytes left",
                vector_idx, vectorHeaderSize<T>() + packed_size + exceptions_size, vectorHeaderSize<T>() + available);

        /// Two separate multiplications, in this order, as the specification requires.
        const T multiplier_factor = Traits::pow10[factor];
        const T multiplier_exponent = Traits::neg_pow10[exponent];

        if (bit_width == 0)
        {
            const T value = static_cast<T>(static_cast<Signed>(frame_of_reference)) * multiplier_factor * multiplier_exponent;
            std::fill(out, out + n, value);
        }
        else
        {
            /// The unpacker may read further than `packed_size` (but not past `max_read_bytes`)
            /// to use wider loads. Everything up to the end of the page is ours to read.
            arrow::internal::unpack<Exact>(
                reinterpret_cast<const uint8_t *>(ptr), deltas,
                {.batch_size = static_cast<int>(n), .bit_width = static_cast<int>(bit_width), .bit_offset = 0,
                 .max_read_bytes = static_cast<int>(std::min(available, size_t(INT_MAX)))});

            /// The deltas are added in unsigned arithmetic (wrapping, as the specification says),
            /// then reinterpreted as signed.
            for (size_t i = 0; i < n; ++i)
                out[i] = static_cast<T>(static_cast<Signed>(deltas[i] + frame_of_reference)) * multiplier_factor * multiplier_exponent;
        }
        ptr += packed_size;

        const char * exception_values = ptr + num_exceptions * sizeof(UInt16);
        for (size_t i = 0; i < num_exceptions; ++i)
        {
            const size_t position = unalignedLoadLittleEndian<UInt16>(ptr + i * sizeof(UInt16));
            if (position >= n)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "ALP exception position {} is out of bounds of vector {} with {} values", position, vector_idx, n);
            /// The exact bits, so that e.g. NaN payloads are preserved.
            out[position] = std::bit_cast<T>(unalignedLoadLittleEndian<Exact>(exception_values + i * sizeof(T)));
        }
    }
};

template <typename T>
struct ALPDecoder : public PageDecoder
{
    using Exact = typename ALPTraits<T>::Exact;

    std::shared_ptr<FixedSizeConverter> converter;
    ALPPage<T> page;
    /// Index of the next value to be decoded or skipped.
    size_t value_idx = 0;

    /// Unpacked integers of the vector being decoded.
    PaddedPODArray<Exact> deltas;
    /// The last vector that was only partially requested, to avoid decoding it again for the
    /// next `decode` call, which usually continues from where the previous one ended.
    PaddedPODArray<T> vector_buffer;
    size_t buffered_vector_idx = SIZE_MAX;
    /// Values for a filter or a non-trivial converter.
    PaddedPODArray<T> temp;

    ALPDecoder(std::span<const char> data_, std::shared_ptr<FixedSizeConverter> converter_)
        : PageDecoder(data_), converter(std::move(converter_)), page(data_)
    {
        if (converter->input_size != sizeof(T))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected input size of converter for ALP: {}", converter->input_size);
    }

    void requireRemainingValues(size_t num_values) const
    {
        if (page.num_elements - value_idx < num_values)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "ALP page has {} values, but more were requested ({} + {})", page.num_elements, value_idx, num_values);
    }

    void skip(size_t num_values) override
    {
        /// Thanks to the offset array, skipped vectors are not even looked at.
        requireRemainingValues(num_values);
        value_idx += num_values;
    }

    void decode(size_t num_values, IColumn & col, const UInt8 * filter, size_t filter_offset) override
    {
        requireRemainingValues(num_values);

        if (!filter)
        {
            if (converter->isTrivial())
            {
                auto span = col.insertRawUninitialized(num_values);
                chassert(span.size() == num_values * sizeof(T));
                decodeRange(num_values, reinterpret_cast<T *>(span.data()));
            }
            else
            {
                temp.resize(num_values);
                decodeRange(num_values, temp.data());
                converter->convertColumn(std::span(reinterpret_cast<const char *>(temp.data()), num_values * sizeof(T)), num_values, col);
            }
            return;
        }

        temp.resize(num_values);
        decodeRange(num_values, temp.data());

        size_t pass_count = 0;
        for (size_t i = 0; i < num_values; ++i)
        {
            temp[pass_count] = temp[i];
            pass_count += filter[filter_offset + i] != 0;
        }

        if (converter->isTrivial())
        {
            auto span = col.insertRawUninitialized(pass_count);
            memcpy(span.data(), temp.data(), pass_count * sizeof(T));
        }
        else
        {
            converter->convertColumn(std::span(reinterpret_cast<const char *>(temp.data()), pass_count * sizeof(T)), pass_count, col);
        }
    }

    /// Decodes the next `num_values` values to `out` and advances `value_idx`.
    void decodeRange(size_t num_values, T * out)
    {
        while (num_values > 0)
        {
            const size_t vector_idx = value_idx >> page.log_vector_size;
            const size_t offset_in_vector = value_idx - (vector_idx << page.log_vector_size);
            const size_t vector_length = page.vectorLength(vector_idx);
            const size_t n = std::min(num_values, vector_length - offset_in_vector);

            if (deltas.empty())
                deltas.resize(std::min(page.vector_size, page.num_elements));

            if (offset_in_vector == 0 && n == vector_length)
            {
                page.decodeVector(vector_idx, out, deltas.data());
            }
            else
            {
                if (buffered_vector_idx != vector_idx)
                {
                    vector_buffer.resize(deltas.size());
                    /// Make the buffer invalid while it's being overwritten, in case of exception.
                    buffered_vector_idx = SIZE_MAX;
                    page.decodeVector(vector_idx, vector_buffer.data(), deltas.data());
                    buffered_vector_idx = vector_idx;
                }
                memcpy(out, vector_buffer.data() + offset_in_vector, n * sizeof(T));
            }

            out += n;
            value_idx += n;
            num_values -= n;
        }
    }
};


/// Writing.

struct ExponentAndFactor
{
    UInt8 exponent = 0;
    UInt8 factor = 0;
};

/// How many values are sampled from a vector when choosing its parameters among the candidates.
constexpr size_t SAMPLES_PER_VECTOR = 64;
/// How many values are sampled from each of the sampled vectors of a page to find the candidates.
constexpr size_t SAMPLES_PER_VECTOR_FOR_CANDIDATES = 256;
/// How many vectors of a page are sampled to find the candidates.
constexpr size_t SAMPLE_VECTORS = 8;
/// How many candidates are kept.
constexpr size_t MAX_CANDIDATES = 5;
/// Stop trying the candidates for a vector after this many in a row did not improve the size.
constexpr size_t MAX_CANDIDATES_WITHOUT_IMPROVEMENT = 2;

template <typename T>
class ALPPageEncoder
{
public:
    using Traits = ALPTraits<T>;
    using Exact = typename Traits::Exact;
    using Signed = typename Traits::Signed;

    /// Encodes the page into `out` (replacing its contents). Returns false, leaving `out` in an
    /// unspecified state, if the encoded page would be bigger than the values in `PLAIN`.
    bool encodePage(const T * values, size_t num_values, PaddedPODArray<char> & out)
    {
        /// A page may have only definition and repetition levels. Don't choose the candidates
        /// without values, leave that to the first page that has them.
        if (num_values == 0)
            return false;

        /// The candidate parameters are chosen from the first page of the column chunk, as the
        /// specification suggests. If a page doesn't compress with them, it is tried again with the
        /// candidates chosen from this page, and those are kept for the next pages.
        if (!candidates.empty() && tryEncodePage(values, num_values, out))
            return true;
        findCandidates(values, num_values);
        if (tryEncodePage(values, num_values, out))
            return true;
        candidates.clear();
        return false;
    }

private:
    std::vector<ExponentAndFactor> candidates;

    PaddedPODArray<Signed> encoded;
    PaddedPODArray<UInt8> is_exception;
    PaddedPODArray<UInt16> exception_positions;
    PaddedPODArray<T> sample;

    /// Encodes the page with the current candidates. Returns false if it's bigger than `PLAIN`.
    bool tryEncodePage(const T * values, size_t num_values, PaddedPODArray<char> & out)
    {
        const size_t num_vectors = (num_values + ALP_ENCODER_VECTOR_SIZE - 1) / ALP_ENCODER_VECTOR_SIZE;
        const size_t plain_size = num_values * sizeof(T);

        out.resize(ALP_PAGE_HEADER_SIZE + num_vectors * 4);
        out[0] = static_cast<char>(ALP_COMPRESSION_MODE);
        out[1] = static_cast<char>(ALP_INTEGER_ENCODING_FOR_BIT_PACKING);
        out[2] = static_cast<char>(ALP_ENCODER_LOG_VECTOR_SIZE);
        if (num_values > INT32_MAX)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many values for an ALP page: {}", num_values);
        unalignedStoreLittleEndian<Int32>(out.data() + 3, static_cast<Int32>(num_values));

        for (size_t vector_idx = 0; vector_idx < num_vectors; ++vector_idx)
        {
            /// `plain_size` is below 2^31, so the offsets fit.
            if (out.size() > plain_size)
                return false;
            unalignedStoreLittleEndian<UInt32>(
                out.data() + ALP_PAGE_HEADER_SIZE + vector_idx * 4, static_cast<UInt32>(out.size() - ALP_PAGE_HEADER_SIZE));

            const size_t begin = vector_idx * ALP_ENCODER_VECTOR_SIZE;
            const size_t n = std::min(ALP_ENCODER_VECTOR_SIZE, num_values - begin);
            encodeVector(values + begin, n, chooseParameters(values + begin, n), out);
        }

        return out.size() <= plain_size;
    }

    /// Takes up to `max_samples` values spread over [values, values + n): one value from each of
    /// the consecutive blocks of `stride` values, at a pseudo-random position inside the block.
    /// With a fixed position the sample would alias with periodic data: for example, for values
    /// with a step of 0.25 and a stride of 16 it would contain only the integers, and the
    /// parameters chosen on it would make 3/4 of the vector exceptions.
    void takeSample(const T * values, size_t n, size_t max_samples)
    {
        const size_t stride = std::max<size_t>(1, (n + max_samples - 1) / max_samples);
        sample.clear();
        for (size_t block_idx = 0, begin = 0; begin < n; ++block_idx, begin += stride)
            sample.push_back(values[begin + offsetInBlock(block_idx, std::min(stride, n - begin))]);
    }

    /// A pseudo-random position inside the block number `block_idx` of `block_size` elements.
    /// Fibonacci hashing of the block number: deterministic and well spread.
    static size_t offsetInBlock(size_t block_idx, size_t block_size)
    {
        return ((block_idx * 0x9E3779B97F4A7C15ULL) >> 32) % block_size;
    }

    /// Encodes one value as the specification describes, then decodes it back. The result is an
    /// exception unless the decoded value is bit-exactly the original (which also catches NaN, the
    /// infinities, -0.0, and the values out of range of the integer type).
    static ALWAYS_INLINE Signed encodeValue(T value, T encode_multiplier_exponent, T encode_multiplier_factor,
        T decode_multiplier_factor, T decode_multiplier_exponent, bool & exception)
    {
        const T scaled = value * encode_multiplier_exponent * encode_multiplier_factor;
        /// Fast rounding to the nearest integer. Wrong for values too big for it, but then the
        /// value doesn't survive the round trip and is stored as an exception anyway.
        T rounded = (scaled + Traits::magic) - Traits::magic;
        /// The comparison is false for NaN.
        const bool in_range = rounded >= Traits::min_encodable && rounded < Traits::max_encodable_exclusive;
        rounded = in_range ? rounded : T(0);
        const Signed result = static_cast<Signed>(rounded);
        const T decoded = static_cast<T>(result) * decode_multiplier_factor * decode_multiplier_exponent;
        exception = std::bit_cast<Exact>(decoded) != std::bit_cast<Exact>(value);
        return result;
    }

    /// Estimated size in bits of the encoded `sample` (bit-packed values and exceptions).
    /// Returns nullopt if `require_two_encodable` and fewer than two values are not exceptions.
    std::optional<size_t> estimateSize(ExponentAndFactor params, bool require_two_encodable) const
    {
        const T encode_multiplier_exponent = Traits::pow10[params.exponent];
        const T encode_multiplier_factor = Traits::neg_pow10[params.factor];
        const T decode_multiplier_factor = Traits::pow10[params.factor];
        const T decode_multiplier_exponent = Traits::neg_pow10[params.exponent];

        size_t num_exceptions = 0;
        Signed min = std::numeric_limits<Signed>::max();
        Signed max = std::numeric_limits<Signed>::min();
        for (T value : sample)
        {
            bool exception = false;
            const Signed x = encodeValue(value, encode_multiplier_exponent, encode_multiplier_factor,
                decode_multiplier_factor, decode_multiplier_exponent, exception);
            num_exceptions += exception;
            min = exception ? min : std::min(min, x);
            max = exception ? max : std::max(max, x);
        }

        const size_t num_encodable = sample.size() - num_exceptions;
        if (require_two_encodable && num_encodable < 2)
            return std::nullopt;

        size_t bit_width = 0;
        if (num_encodable > 0)
            bit_width = std::bit_width(static_cast<UInt64>(static_cast<Exact>(max) - static_cast<Exact>(min)));
        return sample.size() * bit_width + num_exceptions * (sizeof(UInt16) + sizeof(T)) * 8;
    }

    /// The first level of sampling from the paper: an exhaustive search of the parameters on a
    /// sample of a few vectors of the page. The parameters that are the best for the most vectors
    /// become the candidates.
    void findCandidates(const T * values, size_t num_values)
    {
        candidates.clear();

        /// Number of times each combination was the best one for a sampled vector.
        UInt32 wins[Traits::max_exponent + 1][Traits::max_exponent + 1] = {};

        const size_t num_vectors = (num_values + ALP_ENCODER_VECTOR_SIZE - 1) / ALP_ENCODER_VECTOR_SIZE;
        /// One vector from each of the consecutive blocks of `vector_stride` vectors, at a pseudo-random
        /// position inside the block, for the same reason as the values in `takeSample`: with a fixed
        /// position the sampled vectors would alias with the data that alternates between vectors.
        const size_t vector_stride = std::max<size_t>(1, (num_vectors + SAMPLE_VECTORS - 1) / SAMPLE_VECTORS);
        for (size_t block_idx = 0, block_begin = 0; block_begin < num_vectors; ++block_idx, block_begin += vector_stride)
        {
            const size_t vector_idx = block_begin + offsetInBlock(block_idx, std::min(vector_stride, num_vectors - block_begin));
            const size_t begin = vector_idx * ALP_ENCODER_VECTOR_SIZE;
            takeSample(values + begin, std::min(ALP_ENCODER_VECTOR_SIZE, num_values - begin), SAMPLES_PER_VECTOR_FOR_CANDIDATES);

            std::optional<ExponentAndFactor> best;
            size_t best_size = 0;
            for (UInt8 exponent = 0; exponent <= Traits::max_exponent; ++exponent)
            {
                for (UInt8 factor = 0; factor <= exponent; ++factor)
                {
                    const ExponentAndFactor params{exponent, factor};
                    auto size = estimateSize(params, /*require_two_encodable=*/ true);
                    /// On ties, prefer the smaller exponent and factor (unlike the paper). The ties are
                    /// mostly between the combinations with the same difference of the exponent and
                    /// the factor, which give the same integers, and the extra multiplications by the
                    /// inexact powers of ten only add the rounding errors. For example, for `Float32`
                    /// values with a step of 0.25, (2, 0) makes no exceptions, but (6, 4) makes them
                    /// for 5% of the values, which a sample of a vector may not contain.
                    if (size && (!best || *size < best_size))
                    {
                        best = params;
                        best_size = *size;
                    }
                }
            }
            if (best)
                ++wins[best->exponent][best->factor];
        }

        struct Candidate
        {
            ExponentAndFactor params;
            UInt32 wins = 0;
        };
        std::vector<Candidate> found;
        for (UInt8 exponent = 0; exponent <= Traits::max_exponent; ++exponent)
            for (UInt8 factor = 0; factor <= exponent; ++factor)
                if (wins[exponent][factor])
                    found.push_back({{exponent, factor}, wins[exponent][factor]});

        /// The most wins first, on ties the smaller exponent and factor first.
        std::sort(found.begin(), found.end(), [](const Candidate & a, const Candidate & b)
        {
            return std::tuple(a.wins, b.params.exponent, b.params.factor) > std::tuple(b.wins, a.params.exponent, a.params.factor);
        });
        for (size_t i = 0; i < found.size() && i < MAX_CANDIDATES; ++i)
            candidates.push_back(found[i].params);

        /// Nothing is encodable in the sample (or the page is empty): any parameters will do.
        if (candidates.empty())
            candidates.push_back({0, 0});
    }

    /// The second level of sampling: choose the best candidate for a vector using a sample of it.
    ExponentAndFactor chooseParameters(const T * values, size_t n)
    {
        if (candidates.size() == 1)
            return candidates[0];

        takeSample(values, n, SAMPLES_PER_VECTOR);

        ExponentAndFactor best = candidates[0];
        size_t best_size = SIZE_MAX;
        size_t without_improvement = 0;
        for (const auto & params : candidates)
        {
            const size_t size = *estimateSize(params, /*require_two_encodable=*/ false);
            if (size < best_size)
            {
                best = params;
                best_size = size;
                without_improvement = 0;
            }
            else if (++without_improvement == MAX_CANDIDATES_WITHOUT_IMPROVEMENT)
            {
                break;
            }
        }
        return best;
    }

    /// Appends the vector to `out`.
    void encodeVector(const T * __restrict values, size_t n, ExponentAndFactor params, PaddedPODArray<char> & out)
    {
        const T encode_multiplier_exponent = Traits::pow10[params.exponent];
        const T encode_multiplier_factor = Traits::neg_pow10[params.factor];
        const T decode_multiplier_factor = Traits::pow10[params.factor];
        const T decode_multiplier_exponent = Traits::neg_pow10[params.exponent];

        encoded.resize(n);
        is_exception.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            bool exception = false;
            encoded[i] = encodeValue(values[i], encode_multiplier_exponent, encode_multiplier_factor,
                decode_multiplier_factor, decode_multiplier_exponent, exception);
            is_exception[i] = exception;
        }

        /// Branchless compaction of the exception positions.
        exception_positions.resize(n);
        size_t num_exceptions = 0;
        for (size_t i = 0; i < n; ++i)
        {
            exception_positions[num_exceptions] = static_cast<UInt16>(i);
            num_exceptions += is_exception[i];
        }

        /// Replace the exceptions with an encodable value, so that they don't widen the range.
        size_t first_encodable = 0;
        while (first_encodable < num_exceptions && exception_positions[first_encodable] == first_encodable)
            ++first_encodable;
        const Signed placeholder = first_encodable < n ? encoded[first_encodable] : 0;
        for (size_t i = 0; i < num_exceptions; ++i)
            encoded[exception_positions[i]] = placeholder;

        Signed min = encoded[0];
        Signed max = encoded[0];
        for (size_t i = 1; i < n; ++i)
        {
            min = std::min(min, encoded[i]);
            max = std::max(max, encoded[i]);
        }
        const Exact frame_of_reference = static_cast<Exact>(min);
        const UInt8 bit_width = static_cast<UInt8>(std::bit_width(static_cast<UInt64>(static_cast<Exact>(max) - frame_of_reference)));

        const size_t packed_size = (n * bit_width + 7) / 8;
        const size_t vector_size = vectorHeaderSize<T>() + packed_size + num_exceptions * (sizeof(UInt16) + sizeof(T));
        const size_t vector_start = out.size();
        out.resize(vector_start + vector_size);
        char * ptr = out.data() + vector_start;

        ptr[0] = static_cast<char>(params.exponent);
        ptr[1] = static_cast<char>(params.factor);
        unalignedStoreLittleEndian<UInt16>(ptr + 2, static_cast<UInt16>(num_exceptions));
        unalignedStoreLittleEndian<Exact>(ptr + 4, frame_of_reference);
        ptr[4 + sizeof(T)] = static_cast<char>(bit_width);
        ptr += vectorHeaderSize<T>();

        if (bit_width > 0)
        {
            /// LSB-first bit packing of the differences from the frame of reference. The 64-bit
            /// stores may write up to 7 bytes past `packed_size`, which is fine: `out` is padded,
            /// and the bytes after the packed values are overwritten below.
            UInt64 accumulator = 0;
            size_t accumulated_bits = 0;
            char * pos = ptr;
            for (size_t i = 0; i < n; ++i)
            {
                const UInt64 delta = static_cast<Exact>(static_cast<Exact>(encoded[i]) - frame_of_reference);
                accumulator |= delta << accumulated_bits;
                accumulated_bits += bit_width;
                if (accumulated_bits >= 64)
                {
                    unalignedStoreLittleEndian<UInt64>(pos, accumulator);
                    pos += 8;
                    accumulated_bits -= 64;
                    /// The high bits of `delta` that did not fit.
                    accumulator = accumulated_bits ? delta >> (bit_width - accumulated_bits) : 0;
                }
            }
            if (accumulated_bits)
                unalignedStoreLittleEndian<UInt64>(pos, accumulator);
            chassert(size_t(pos - ptr) + (accumulated_bits + 7) / 8 == packed_size);
        }
        ptr += packed_size;

        for (size_t i = 0; i < num_exceptions; ++i)
            unalignedStoreLittleEndian<UInt16>(ptr + i * sizeof(UInt16), exception_positions[i]);
        ptr += num_exceptions * sizeof(UInt16);
        for (size_t i = 0; i < num_exceptions; ++i)
            unalignedStoreLittleEndian<Exact>(ptr + i * sizeof(T), std::bit_cast<Exact>(values[exception_positions[i]]));
    }
};

/// Adapts `ALPPageEncoder` to the encoder interface of Arrow that the writer uses for other encodings.
template <typename DType>
class ALPEncoder final : public parquet::TypedEncoder<DType>
{
public:
    using T = typename DType::c_type;
    using parquet::TypedEncoder<DType>::Put;

    int64_t EstimatedDataEncodedSize() override
    {
        return static_cast<int64_t>(values.size() * sizeof(T));
    }

    std::shared_ptr<parquet::Buffer> FlushValues() override
    {
        std::shared_ptr<parquet::Buffer> result;
        if (page_encoder.encodePage(values.data(), values.size(), encoded))
        {
            last_page_encoding = static_cast<parquet::Encoding::type>(parq::Encoding::ALP);
            result = arrow::Buffer::FromString(std::string(encoded.data(), encoded.size()));
        }
        else
        {
            last_page_encoding = parquet::Encoding::PLAIN;
            result = arrow::Buffer::FromString(std::string(reinterpret_cast<const char *>(values.data()), values.size() * sizeof(T)));
        }
        values.clear();
        return result;
    }

    parquet::Encoding::type encoding() const override { return last_page_encoding; }

    void Put(const T * src, int num_values) override
    {
        values.insert(src, src + num_values);
    }

    void Put(const arrow::Array &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Putting an Arrow array into ALP encoder is not implemented");
    }

    void PutSpaced(const T *, int, const uint8_t *, int64_t) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Putting spaced values into ALP encoder is not implemented");
    }

    int64_t ReportUnencodedDataBytes() override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ReportUnencodedDataBytes is only for BYTE_ARRAY");
    }

    arrow::MemoryPool * memory_pool() const override { return arrow::default_memory_pool(); }

private:
    PODArray<T> values;
    ALPPageEncoder<T> page_encoder;
    PaddedPODArray<char> encoded;
    parquet::Encoding::type last_page_encoding = static_cast<parquet::Encoding::type>(parq::Encoding::ALP);
};

}

std::unique_ptr<PageDecoder> makeALPDecoder(
    parq::Type::type physical_type, std::span<const char> data, std::shared_ptr<FixedSizeConverter> converter)
{
    switch (physical_type)
    {
        case parq::Type::FLOAT: return std::make_unique<ALPDecoder<float>>(data, std::move(converter));
        case parq::Type::DOUBLE: return std::make_unique<ALPDecoder<double>>(data, std::move(converter));
        default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected encoding ALP for type {}", thriftToString(physical_type));
    }
}

std::unique_ptr<parquet::TypedEncoder<parquet::FloatType>> makeALPFloatEncoder()
{
    return std::make_unique<ALPEncoder<parquet::FloatType>>();
}

std::unique_ptr<parquet::TypedEncoder<parquet::DoubleType>> makeALPDoubleEncoder()
{
    return std::make_unique<ALPEncoder<parquet::DoubleType>>();
}

}
