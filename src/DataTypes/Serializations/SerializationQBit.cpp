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

#include <base/BFloat16.h>
#include <base/types.h>


namespace DB
{

namespace ErrorCodes
{
extern const int SERIALIZATION_ERROR;
extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH;
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

template <typename Word, typename Val>
void SerializationQBit::serializeFloatsFromQBitTuple(const Tuple & tuple, WriteBuffer & ostr) const
{
    constexpr size_t bits = sizeof(Word) * 8;
    const size_t slice_size = DataTypeQBit::bitsToBytes(dimension);
    const size_t slice_size_bits = slice_size * 8;
    std::vector<Val> dst(slice_size_bits, Val{});

    for (size_t bit = 0; bit < bits; ++bit)
    {
        const String & fixed_string = tuple[bit].safeGet<String>();
        const UInt8 * src = reinterpret_cast<const UInt8 *>(fixed_string.data());
        const Word mask = Word(1) << (bits - 1 - bit);
        SerializationQBit::untransposeBitPlane(src, reinterpret_cast<Word *>(dst.data()), slice_size_bits, mask);
    }

    writeVectorBinary(dst, ostr);
}

template <typename FloatType>
Tuple SerializationQBit::deserializeFloatsToQBitTuple(ReadBuffer & istr) const
{
    using Word = std::conditional_t<sizeof(FloatType) == 2, UInt16, std::conditional_t<sizeof(FloatType) == 4, UInt32, UInt64>>;

    const size_t bytes_per_fixedstring = DataTypeQBit::bitsToBytes(dimension);
    const size_t padded_n = bytes_per_fixedstring * 8;

    std::vector<FloatType> value_floats(padded_n);
    readVectorBinary(value_floats, istr);

    const char * value_bytes = reinterpret_cast<const char *>(value_floats.data());

    /// Transpose data
    std::vector<char> transposed_bytes(bytes_per_fixedstring * element_size);
    transposeBits<Word>(reinterpret_cast<const Word *>(value_bytes), reinterpret_cast<Word *>(transposed_bytes.data()), padded_n);

    /// Create tuple of FixedStrings
    Tuple tuple_elements;
    tuple_elements.reserve(element_size);

    for (size_t col_idx = 0; col_idx < element_size; ++col_idx)
    {
        String fixed_string(transposed_bytes.data() + col_idx * bytes_per_fixedstring, bytes_per_fixedstring);
        tuple_elements.push_back(Field(std::move(fixed_string)));
    }

    return tuple_elements;
}

template <typename Word, typename Val, typename WriteFunc>
void SerializationQBit::serializeFloatsFromQBit(const IColumn & column, size_t row_num, WriteFunc && write_func) const
{
    constexpr size_t bits = sizeof(Word) * 8;
    const size_t slice_size = DataTypeQBit::bitsToBytes(dimension);
    const size_t slice_size_bits = slice_size * 8;

    std::vector<Val> dst(slice_size_bits, Val{});

    for (size_t bit = 0; bit < bits; ++bit)
    {
        const auto & fs = assert_cast<const ColumnFixedString &>(extractElementColumn(column, bit));
        const UInt8 * src = reinterpret_cast<const UInt8 *>(fs.getChars().data()) + row_num * slice_size;
        const Word mask = Word(1) << (bits - 1 - bit);
        SerializationQBit::untransposeBitPlane(src, reinterpret_cast<Word *>(dst.data()), slice_size_bits, mask);
    }

    write_func(dst);
}

template <typename FloatType, typename ReadFunc>
void SerializationQBit::deserializeFloatsToQBit(IColumn & column, ReadFunc && read_func) const
{
    const size_t bytes_per_fixedstring = DataTypeQBit::bitsToBytes(dimension);
    const size_t padded_n = bytes_per_fixedstring * 8;

    std::vector<FloatType> value_floats(padded_n);
    read_func(value_floats);

    const char * value_bytes = reinterpret_cast<const char *>(value_floats.data());

    /// Transpose the data
    std::vector<char> transposed_bytes(bytes_per_fixedstring * element_size);

    if (element_size == 16)
        transposeBits<UInt16>(reinterpret_cast<const UInt16 *>(value_bytes), reinterpret_cast<UInt16 *>(transposed_bytes.data()), padded_n);
    else if (element_size == 32)
        transposeBits<UInt32>(reinterpret_cast<const UInt32 *>(value_bytes), reinterpret_cast<UInt32 *>(transposed_bytes.data()), padded_n);
    else if (element_size == 64)
        transposeBits<UInt64>(reinterpret_cast<const UInt64 *>(value_bytes), reinterpret_cast<UInt64 *>(transposed_bytes.data()), padded_n);

    /// Insert the transposed data into columns
    for (size_t col_idx = 0; col_idx < element_size; ++col_idx)
    {
        auto & element_column = extractElementColumn(column, col_idx);
        auto & fixed_string_column = assert_cast<ColumnFixedString &>(element_column);
        fixed_string_column.insertData(transposed_bytes.data() + col_idx * bytes_per_fixedstring, bytes_per_fixedstring);
    }
}

void SerializationQBit::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    /// Tuple<String, ..., String>
    const Tuple & tuple = field.safeGet<Tuple>();

    if (tuple.size() != element_size)
        throw Exception(
            ErrorCodes::SERIALIZATION_ERROR, "QBit tuple size {} doesn't match expected element_size {}", tuple.size(), element_size);

    if (element_size == 16)
        serializeFloatsFromQBitTuple<UInt16, BFloat16>(tuple, ostr);
    else if (element_size == 32)
        serializeFloatsFromQBitTuple<UInt32, Float32>(tuple, ostr);
    else if (element_size == 64)
        serializeFloatsFromQBitTuple<UInt64, Float64>(tuple, ostr);
    else
        throw Exception(ErrorCodes::SERIALIZATION_ERROR, "Unsupported QBit element size {}", element_size);
}

void SerializationQBit::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    if (element_size == 16)
        field = deserializeFloatsToQBitTuple<BFloat16>(istr);
    else if (element_size == 32)
        field = deserializeFloatsToQBitTuple<Float32>(istr);
    else if (element_size == 64)
        field = deserializeFloatsToQBitTuple<Float64>(istr);
    else
        throw Exception(ErrorCodes::SERIALIZATION_ERROR, "Unsupported QBit element size {}", element_size);
}

void SerializationQBit::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    /// Lambda to write the vector of floats to the output buffer
    auto write_binary = [&ostr](const auto & dst) { writeVectorBinary(dst, ostr); };

    if (element_size == 16)
        serializeFloatsFromQBit<UInt16, BFloat16>(column, row_num, write_binary);
    else if (element_size == 32)
        serializeFloatsFromQBit<UInt32, Float32>(column, row_num, write_binary);
    else if (element_size == 64)
        serializeFloatsFromQBit<UInt64, Float64>(column, row_num, write_binary);
    else
        throw Exception(ErrorCodes::SERIALIZATION_ERROR, "Unsupported QBit size {}. Only 16, 32, and 64 are supported", element_size);
}

void SerializationQBit::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    /// Lambda to read the vector of floats from the buffer
    auto read_binary = [&istr](auto & value_floats) { readVectorBinary(value_floats, istr); };

    auto deserialize = [&]() -> bool
    {
        if (element_size == 16)
            deserializeFloatsToQBit<BFloat16>(column, read_binary);
        else if (element_size == 32)
            deserializeFloatsToQBit<Float32>(column, read_binary);
        else if (element_size == 64)
            deserializeFloatsToQBit<Float64>(column, read_binary);
        else
            throw Exception(
                ErrorCodes::SERIALIZATION_ERROR, "Unsupported size for QBit: {}. Only 16, 32, and 64 are supported", element_size);
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
    if (element_size == 16)
        serializeFloatsFromQBit<UInt16, BFloat16>(column, row_num, write_text);
    else if (element_size == 32)
        serializeFloatsFromQBit<UInt32, Float32>(column, row_num, write_text);
    else if (element_size == 64)
        serializeFloatsFromQBit<UInt64, Float64>(column, row_num, write_text);
    else
        throw Exception(ErrorCodes::SERIALIZATION_ERROR, "Unsupported QBit size {}. Only 16, 32, and 64 are supported", element_size);
    writeChar(']', ostr);
}

void SerializationQBit::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    /// Lambda to read comma-separated floats from the buffer
    auto read_with_comma = [this, &istr](auto & value_floats)
    {
        for (size_t i = 0; i < dimension; ++i)
        {
            if (i != 0)
            {
                skipWhitespaceIfAny(istr);
                assertChar(',', istr);
                skipWhitespaceIfAny(istr);
            }
            readText(value_floats[i], istr);
        }
    };

    auto deserialize = [&]() -> bool
    {
        assertChar('[', istr);
        skipWhitespaceIfAny(istr);

        if (element_size == 16)
            deserializeFloatsToQBit<BFloat16>(column, read_with_comma);
        else if (element_size == 32)
            deserializeFloatsToQBit<Float32>(column, read_with_comma);
        else if (element_size == 64)
            deserializeFloatsToQBit<Float64>(column, read_with_comma);
        else
            throw Exception(
                ErrorCodes::SERIALIZATION_ERROR, "Unsupported size for QBit: {}. Only 16, 32, and 64 are supported", element_size);

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

template <typename T>
void SerializationQBit::transposeBits(const T * src, T * dst, std::size_t length)
{
    constexpr std::size_t w = sizeof(T) * 8;

    std::fill(dst, dst + length, T{0});

    constexpr std::size_t logw = (w == 16) ? 4 : (w == 32) ? 5 : 6; /// log2(w)
    constexpr std::size_t maskw = w - 1;
    const std::size_t total_bits = w * length;

    for (std::size_t bit = 0; bit < w; ++bit)
    {
        for (std::size_t row = 0; row < length; ++row)
        {
            T v = (src[row] >> bit) & T{1};

            std::size_t dst_index = total_bits - 1 - (bit * length + row);

            std::size_t dst_word = dst_index >> logw;
            std::size_t dst_offset = dst_index & maskw;

            dst[dst_word] |= (v << dst_offset);
        }
    }
}

template <typename T>
ALWAYS_INLINE void SerializationQBit::untransposeBitPlane(const UInt8 * __restrict src, T * __restrict dst, size_t stride_len, T bit_mask)
{
    const size_t bytes_per_slice = stride_len / 8;

    for (size_t b = 0; b < bytes_per_slice; ++b)
    {
        uint8_t v = src[b];

        /// Eealy out for common all-0 case
        if (!v)
            continue;

        ssize_t row_base = static_cast<ssize_t>(stride_len - 1 - (b * 8));
        T * d = dst + row_base;

        /** Map     LSB → row_base
          *         …   → …
          *         MSB → row_base-7
          */
        if (v & 0x01)
            d[0] |= bit_mask;
        if (v & 0x02)
            d[-1] |= bit_mask;
        if (v & 0x04)
            d[-2] |= bit_mask;
        if (v & 0x08)
            d[-3] |= bit_mask;
        if (v & 0x10)
            d[-4] |= bit_mask;
        if (v & 0x20)
            d[-5] |= bit_mask;
        if (v & 0x40)
            d[-6] |= bit_mask;
        if (v & 0x80)
            d[-7] |= bit_mask;
    }
}


template void SerializationQBit::transposeBits<UInt16>(const UInt16 *, UInt16 *, size_t);
template void SerializationQBit::transposeBits<UInt32>(const UInt32 *, UInt32 *, size_t);
template void SerializationQBit::transposeBits<UInt64>(const UInt64 *, UInt64 *, size_t);

template void SerializationQBit::untransposeBitPlane(const UInt8 * src, UInt16 * dst, size_t stride_len, UInt16 bit_mask);
template void SerializationQBit::untransposeBitPlane(const UInt8 * src, UInt32 * dst, size_t stride_len, UInt32 bit_mask);
template void SerializationQBit::untransposeBitPlane(const UInt8 * src, UInt64 * dst, size_t stride_len, UInt64 bit_mask);
}
