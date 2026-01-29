#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnTuple.h>

#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/Serializations/SerializationQBit.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <Common/TargetSpecific.h>

#include <base/BFloat16.h>
#include <base/types.h>

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif


namespace DB
{

namespace ErrorCodes
{
extern const int SERIALIZATION_ERROR;
extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH;
extern const int TOO_LARGE_ARRAY_SIZE;
}

static const ColumnTuple & extractNestedColumn(const IColumn & column)
{
    return assert_cast<const ColumnQBit &>(column).getNestedData();
}

static ColumnTuple & extractNestedColumn(IColumn & column)
{
    return assert_cast<ColumnQBit &>(column).getNestedData();
}

/* Get const idx-th column */
static inline const IColumn & extractElementColumn(const IColumn & column, size_t idx)
{
    return extractNestedColumn(column).getColumn(idx);
}

/* Get idx-th column */
static inline IColumn & extractElementColumn(IColumn & column, size_t idx)
{
    return extractNestedColumn(column).getColumn(idx);
}

template <typename ReturnType, typename F>
static ReturnType addElementSafe(size_t num_elems, IColumn & column, F && impl)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// We use the assumption that tuples of zero size do not exist.
    size_t old_size = column.size();

    auto restore_elements = [&]()
    {
        for (size_t i = 0; i < num_elems; ++i)
        {
            auto & element_column = extractElementColumn(column, i);
            if (element_column.size() > old_size)
            {
                chassert(element_column.size() - old_size == 1);
                element_column.popBack(1);
            }
        }
    };

    try
    {
        if (!impl())
        {
            restore_elements();
            return ReturnType(false);
        }

        extractNestedColumn(column).addSize(1);


        /// Check that all columns now have the same size.
        size_t new_size = column.size();
        for (size_t i = 1; i < num_elems; ++i)
        {
            const auto & element_column = extractElementColumn(column, i);
            if (element_column.size() != new_size)
            {
                restore_elements();
                // This is not a logical error because it may work with user-supplied data.
                if constexpr (throw_exception)
                    throw Exception(
                        ErrorCodes::SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH, "Cannot read a tuple because not all elements are present");
                return ReturnType(false);
            }
        }
    }
    catch (...)
    {
        restore_elements();
        if constexpr (throw_exception)
            throw;
        return ReturnType(false);
    }

    return ReturnType(true);
}

size_t SerializationQBit::validateAndReadQBitSize(ReadBuffer & istr, const FormatSettings & settings) const
{
    size_t size;
    readVarUInt(size, istr);

    if (settings.binary.max_binary_array_size && size > settings.binary.max_binary_array_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_ARRAY_SIZE,
            "Too large array size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_array_size",
            size,
            settings.binary.max_binary_array_size);

    if (size != dimension)
        throw Exception(
            ErrorCodes::SERIALIZATION_ERROR, "Dimension of the read QBit {} doesn't match expected dimension {}", size, dimension);

    return size;
}

template <typename Func>
void SerializationQBit::dispatchByElementSize(Func && func) const
{
    if (element_size == 16)
        func.template operator()<BFloat16>();
    else if (element_size == 32)
        func.template operator()<Float32>();
    else if (element_size == 64)
        func.template operator()<Float64>();
    else
        throw Exception(ErrorCodes::SERIALIZATION_ERROR, "Unsupported size for QBit: {}. Only 16, 32, and 64 are supported", element_size);
}

template <typename FloatType>
void SerializationQBit::serializeFloatsFromQBitTuple(const Tuple & tuple, WriteBuffer & ostr) const
{
    using Word = std::conditional_t<sizeof(FloatType) == 2, UInt16, std::conditional_t<sizeof(FloatType) == 4, UInt32, UInt64>>;

    constexpr size_t bits = sizeof(Word) * 8;
    const size_t slice_size = DataTypeQBit::bitsToBytes(dimension);
    const size_t slice_size_bits = slice_size * 8;
    std::vector<FloatType> dst(slice_size_bits, FloatType{});

    for (size_t bit = 0; bit < bits; ++bit)
    {
        const String & fixed_string = tuple[bit].safeGet<String>();
        const UInt8 * src = reinterpret_cast<const UInt8 *>(fixed_string.data());
        const Word mask = Word(1) << (bits - 1 - bit);
        SerializationQBit::untransposeBitPlane(src, reinterpret_cast<Word *>(dst.data()), slice_size_bits, mask);
    }

    /// We untransposed QBit and might have trailing zero floats at the tail if dimension % 8 != 0. Remove them
    dst.resize(dimension);
    writeVectorBinary(dst, ostr);
}

template <typename FloatType>
Tuple SerializationQBit::deserializeFloatsToQBitTuple(ReadBuffer & istr) const
{
    using Word = std::conditional_t<sizeof(FloatType) == 2, UInt16, std::conditional_t<sizeof(FloatType) == 4, UInt32, UInt64>>;

    const size_t bytes_per_fixedstring = DataTypeQBit::bitsToBytes(dimension);
    const size_t total_bits = bytes_per_fixedstring * 8;

    std::vector<std::string> planes(element_size, std::string(bytes_per_fixedstring, '\0'));
    std::vector<char *> plane_ptrs(element_size);
    for (size_t col_idx = 0; col_idx < element_size; ++col_idx)
        plane_ptrs[col_idx] = reinterpret_cast<char *>(planes[col_idx].data());

    Word w;
    FloatType v;

    for (size_t i = 0; i < dimension; i++)
    {
        readFloatBinary(v, istr);
        std::memcpy(&w, &v, sizeof(Word));
        transposeBits<Word>(w, i, total_bits, plane_ptrs.data());
    }

    Tuple tuple_elements;
    tuple_elements.reserve(element_size);

    for (size_t col_idx = 0; col_idx < element_size; ++col_idx)
        tuple_elements.push_back(Field(std::move(planes[col_idx])));

    return tuple_elements;
}

template <typename FloatType, typename WriteFunc>
void SerializationQBit::serializeFloatsFromQBit(const IColumn & column, size_t row_num, WriteFunc && write_func) const
{
    using Word = std::conditional_t<sizeof(FloatType) == 2, UInt16, std::conditional_t<sizeof(FloatType) == 4, UInt32, UInt64>>;

    constexpr size_t bits = sizeof(Word) * 8;
    const size_t slice_size = DataTypeQBit::bitsToBytes(dimension);
    const size_t slice_size_bits = slice_size * 8;

    std::vector<FloatType> dst(slice_size_bits, FloatType{});

    for (size_t bit = 0; bit < bits; ++bit)
    {
        const auto & fs = assert_cast<const ColumnFixedString &>(extractElementColumn(column, bit));
        const UInt8 * src = reinterpret_cast<const UInt8 *>(fs.getChars().data()) + row_num * slice_size;
        const Word mask = Word(1) << (bits - 1 - bit);
        SerializationQBit::untransposeBitPlane(src, reinterpret_cast<Word *>(dst.data()), slice_size_bits, mask);
    }

    /// We untransposed QBit and might have trailing zero floats at the tail if dimension % 8 != 0. Remove them
    dst.resize(dimension);
    write_func(dst);
}

template <typename FloatType, typename ReadFunc>
void SerializationQBit::deserializeFloatsToQBit(IColumn & column, ReadFunc read_one) const
{
    using Word = std::conditional_t<sizeof(FloatType) == 2, UInt16, std::conditional_t<sizeof(FloatType) == 4, UInt32, UInt64>>;

    const size_t bytes_per_fixedstring = DataTypeQBit::bitsToBytes(dimension);
    const size_t total_bits = bytes_per_fixedstring * 8;

    /// Insert 0 in each FixedString column and prepare pointers to the newly inserted rows to directly write into them during transposition
    std::vector<char *> column_data_ptrs(element_size);
    for (size_t col_idx = 0; col_idx < element_size; ++col_idx)
    {
        auto & fixed_string_column = assert_cast<ColumnFixedString &>(extractElementColumn(column, col_idx));
        fixed_string_column.insertDefault();
        auto & chars = fixed_string_column.getChars();
        column_data_ptrs[col_idx] = reinterpret_cast<char *>(&chars[chars.size() - bytes_per_fixedstring]);
    }

    for (size_t i = 0; i < dimension; ++i)
    {
        FloatType value;
        read_one(value, i);

        Word word_value;
        std::memcpy(&word_value, &value, sizeof(Word));

        transposeBits<Word>(word_value, i, total_bits, column_data_ptrs.data());
    }
}

void SerializationQBit::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    /// Tuple<String, ..., String>
    const Tuple & tuple = field.safeGet<Tuple>();

    if (tuple.size() != element_size)
        throw Exception(
            ErrorCodes::SERIALIZATION_ERROR, "QBit tuple size {} doesn't match expected element_size {}", tuple.size(), element_size);

    dispatchByElementSize([&]<typename FloatType>() { serializeFloatsFromQBitTuple<FloatType>(tuple, ostr); });
}

void SerializationQBit::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    validateAndReadQBitSize(istr, settings);

    dispatchByElementSize([&]<typename FloatType>() { field = deserializeFloatsToQBitTuple<FloatType>(istr); });
}

void SerializationQBit::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    /// Lambda to write the vector of floats to the output buffer
    auto write_binary = [&ostr](const auto & dst) { writeVectorBinary(dst, ostr); };

    dispatchByElementSize([&]<typename FloatType>() { serializeFloatsFromQBit<FloatType>(column, row_num, write_binary); });
}

void SerializationQBit::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    validateAndReadQBitSize(istr, settings);

    auto read_binary = [&]<typename FloatType>(FloatType & v, size_t) { readBinary(v, istr); };

    auto deserialize = [&]() -> bool
    {
        dispatchByElementSize([&]<typename FloatType>() { deserializeFloatsToQBit<FloatType>(column, read_binary); });
        return true;
    };

    addElementSafe<void>(element_size, column, deserialize);
}

void SerializationQBit::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    /// Lambda to write comma-separated floats to the output buffer
    auto write_text = [this, &ostr](const auto & dst)
    {
        for (size_t i = 0; i < dimension; ++i)
        {
            if (i > 0)
                writeChar(',', ostr);
            writeText(dst[i], ostr);
        }
    };

    writeChar('[', ostr);
    dispatchByElementSize([&]<typename FloatType>() { serializeFloatsFromQBit<FloatType>(column, row_num, write_text); });
    writeChar(']', ostr);
}

void SerializationQBit::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    /// Lambda to read comma-separated floats from the buffer
    auto read_with_comma = [&]<typename FloatType>(FloatType & v, size_t i)
    {
        if (i != 0)
        {
            skipWhitespaceIfAny(istr);
            assertChar(',', istr);
            skipWhitespaceIfAny(istr);
        }
        readText(v, istr);
    };

    auto deserialize = [&]() -> bool
    {
        assertChar('[', istr);
        skipWhitespaceIfAny(istr);

        dispatchByElementSize([&]<typename FloatType>() { deserializeFloatsToQBit<FloatType>(column, read_with_comma); });

        skipWhitespaceIfAny(istr);
        assertChar(']', istr);

        if (whole && !istr.eof())
            throwUnexpectedDataAfterParsedValue(column, istr, settings, "QBit");

        return true;
    };

    addElementSafe<void>(element_size, column, deserialize);
}

void SerializationQBit::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    auto next_data = SubstreamData(nested)
                         .withType(data.type ? assert_cast<const DataTypeQBit &>(*data.type).getNestedType() : nullptr)
                         .withColumn(data.column ? assert_cast<const ColumnQBit &>(*data.column).getTuple() : nullptr)
                         .withSerializationInfo(data.serialization_info)
                         .withDeserializeState(data.deserialize_state);

    nested->enumerateStreams(settings, callback, next_data);
}

void SerializationQBit::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStatePrefix(extractNestedColumn(column), settings, state);
}

void SerializationQBit::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationQBit::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    nested->deserializeBinaryBulkStatePrefix(settings, state, cache);
}

void SerializationQBit::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkWithMultipleStreams(extractNestedColumn(column), offset, limit, settings, state);
}

void SerializationQBit::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    const auto & column_qbit = assert_cast<const ColumnQBit &>(*column);
    ColumnPtr tuple = column_qbit.getTuple();
    nested->deserializeBinaryBulkWithMultipleStreams(tuple, rows_offset, limit, settings, state, cache);
}

template <typename Word>
void SerializationQBit::transposeBits(Word src, const size_t row_i, const size_t total_bits, char * const * __restrict dst)
{
    /// Fast out on common all-zeros case
    if (!src)
        return;

    /// (row_i ^ 7) maps 0 -> 7, 1 -> 6, ..., 7 -> 0. Same with higher numbers: 15 -> 8, etc. Required for our row ordering
    const size_t bit_index = (total_bits - 1) - (row_i ^ 7);
    const size_t byte_pos = bit_index / 8;
    const uint8_t bit_mask = static_cast<uint8_t>(1u << (bit_index % 8));
    constexpr size_t bits_per_word_minus_one = sizeof(Word) * 8 - 1;

    /// Process only the set bits
    while (src)
    {
        /// Index of the lowest set bit
        auto trailing_zeros = std::countr_zero(src);
        size_t col_idx = bits_per_word_minus_one - trailing_zeros;
        dst[col_idx][byte_pos] |= bit_mask;
        /// Clear that bit
        src &= src - 1;
    }
}

/// CPU Dispatch for untransposeBitPlane
DECLARE_DEFAULT_CODE(
    template <typename T>
    ALWAYS_INLINE inline void untransposeBitPlaneImpl(const UInt8 * __restrict src, T * __restrict dst, size_t stride_len, T bit_mask) {
        const size_t bytes_per_fs = stride_len / 8;
        ssize_t row_base = stride_len - 1;

        for (size_t b = 0; b < bytes_per_fs; ++b, row_base -= 8)
        {
            const uint8_t v = src[b];

            /// Fast out on common all-zeros case
            if (!v)
                continue;

            for (int i = 0; i < 8; ++i)
            {
                /// Mask is 0...0 if current bit is 0, 1...1 if it is 1. Use it to avoid a branch
                T mask = -T((v >> i) & 1);
                dst[row_base - 7 + i] |= (mask & bit_mask);
            }
        }
    })

/// Do not inline target specific implementations to avoid code bloat on all targets
DECLARE_AVX512F_SPECIFIC_CODE(
    void untransposeBitPlaneFloat64Impl(const UInt8 * __restrict src, UInt64 * __restrict dst, size_t stride_len, UInt64 bit_mask) {
        const size_t bytes_per_fs = stride_len / 8;
        ssize_t row_base = stride_len - 1;

        const __m512i bmask = _mm512_set1_epi64(bit_mask);

        for (size_t b = 0; b < bytes_per_fs; ++b, row_base -= 8)
        {
            uint8_t v = src[b];
            if (!v)
                continue;

            __mmask8 k = v;
            uint64_t * row_ptr = dst + row_base - 7;

            __m512i cur = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(row_ptr));
            __m512i upd = _mm512_or_si512(cur, bmask);
            cur = _mm512_mask_mov_epi64(cur, k, upd);
            _mm512_storeu_si512(reinterpret_cast<__m512i *>(row_ptr), cur);
        }
    })

DECLARE_AVX512VL_SPECIFIC_CODE(
    void untransposeBitPlaneFloat32Impl(const UInt8 * __restrict src, UInt32 * __restrict dst, size_t stride_len, UInt32 bit_mask) {
        const size_t bytes_per_fs = stride_len / 8;
        ssize_t row_base = stride_len - 1;

        const __m512i bmask = _mm512_set1_epi32(bit_mask);

        size_t b = 0;

        for (; b + 4 <= bytes_per_fs; b += 4, row_base -= 32)
        {
            uint32_t v32;
            std::memcpy(&v32, src + b, 4);

            if (!v32)
                continue;

            /// Bytes within uint32_t are stored in big-endian format. Swap to little-endian for _cvtu32_mask32.
            v32 = __builtin_bswap32(v32);
            __mmask32 k32 = _cvtu32_mask32(v32);

            __mmask16 k_lo = static_cast<__mmask16>(k32 & 0xFFFF);
            __mmask16 k_hi = static_cast<__mmask16>(k32 >> 16);

            uint32_t * row_ptr = dst + row_base - 31;

            __m512i cur = _mm512_loadu_si512(row_ptr);
            __m512i upd = _mm512_or_si512(cur, bmask);
            cur = _mm512_mask_mov_epi32(cur, k_lo, upd);
            _mm512_storeu_si512(row_ptr, cur);

            cur = _mm512_loadu_si512(row_ptr + 16);
            upd = _mm512_or_si512(cur, bmask);
            cur = _mm512_mask_mov_epi32(cur, k_hi, upd);
            _mm512_storeu_si512(row_ptr + 16, cur);
        }

        for (; b < bytes_per_fs; ++b, row_base -= 8)
        {
            __mmask8 v = src[b];
            if (!v)
                continue;

            uint32_t * rp = dst + row_base - 7; // rows [‑7 … 0]

            __m256i cur256 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(rp));
            __m256i upd256 = _mm256_or_si256(cur256, _mm256_set1_epi32(bit_mask));
            cur256 = _mm256_mask_mov_epi32(cur256, v, upd256);
            _mm256_storeu_si256(reinterpret_cast<__m256i *>(rp), cur256);
        }
    })

DECLARE_AVX512VL_SPECIFIC_CODE(
    void untransposeBitPlaneBFloat16Impl(const UInt8 * __restrict src, UInt16 * __restrict dst, size_t stride_len, UInt16 bit_mask) {
        const size_t bytes_per_fs = stride_len / 8;
        const __m512i bmask = _mm512_set1_epi16(bit_mask);
        ssize_t row_base = stride_len - 1;

        /// Process 4 bytes at a time
        size_t b = 0;
        for (; b + 4 <= bytes_per_fs; b += 4, row_base -= 32)
        {
            uint32_t v32;
            std::memcpy(&v32, src + b, 4);

            if (!v32)
                continue;

            /// Bytes within uint32_t are stored in big-endian format. Swap to little-endian for _cvtu32_mask32.
            v32 = __builtin_bswap32(v32);
            __mmask32 k = _cvtu32_mask32(v32);

            uint16_t * row_ptr = dst + row_base - 31;

            __m512i cur = _mm512_loadu_si512(row_ptr);
            __m512i upd = _mm512_or_si512(cur, bmask);
            cur = _mm512_mask_mov_epi16(cur, k, upd);
            _mm512_storeu_si512(row_ptr, cur);
        }

        /// Process remaining rows
        for (; b < bytes_per_fs; ++b, row_base -= 8)
        {
            uint8_t v = src[b];
            if (!v)
                continue;

            __mmask8 k8 = v;
            uint16_t * rp = dst + row_base - 7;

            __m128i cur = _mm_loadu_si128(reinterpret_cast<__m128i *>(rp));
            __m128i upd = _mm_or_si128(cur, _mm_set1_epi16(bit_mask));
            cur = _mm_mask_mov_epi16(cur, k8, upd);
            _mm_storeu_si128(reinterpret_cast<__m128i *>(rp), cur);
        }
    })

template <typename T>
void SerializationQBit::untransposeBitPlane(const UInt8 * __restrict src, T * __restrict dst, size_t stride_len, T bit_mask)
{
#if USE_MULTITARGET_CODE
    if constexpr (std::is_same_v<T, UInt64>)
    {
        if (isArchSupported(TargetArch::AVX512F))
            return TargetSpecific::AVX512F::untransposeBitPlaneFloat64Impl(src, dst, stride_len, bit_mask);
    }
    else if constexpr (std::is_same_v<T, UInt32>)
    {
        if (isArchSupported(TargetArch::AVX512VL))
            return TargetSpecific::AVX512VL::untransposeBitPlaneFloat32Impl(src, dst, stride_len, bit_mask);
    }
    else if constexpr (std::is_same_v<T, UInt16>)
    {
        if (isArchSupported(TargetArch::AVX512VL))
            return TargetSpecific::AVX512VL::untransposeBitPlaneBFloat16Impl(src, dst, stride_len, bit_mask);
    }
#endif
    return TargetSpecific::Default::untransposeBitPlaneImpl(src, dst, stride_len, bit_mask);
}


template void SerializationQBit::transposeBits(UInt16 src, const size_t row_i, const size_t total_bits, char * const * dst);
template void SerializationQBit::transposeBits(UInt32 src, const size_t row_i, const size_t total_bits, char * const * dst);
template void SerializationQBit::transposeBits(UInt64 src, const size_t row_i, const size_t total_bits, char * const * dst);

template void SerializationQBit::untransposeBitPlane(const UInt8 * src, UInt64 * dst, size_t stride_len, UInt64 bit_mask);
template void SerializationQBit::untransposeBitPlane(const UInt8 * src, UInt32 * dst, size_t stride_len, UInt32 bit_mask);
template void SerializationQBit::untransposeBitPlane(const UInt8 * src, UInt16 * dst, size_t stride_len, UInt16 bit_mask);
}
