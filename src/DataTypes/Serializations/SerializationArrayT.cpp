#include <Columns/ColumnArrayT.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/Serializations/SerializationArrayT.h>
#include <DataTypes/Serializations/SerializationFixedString.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/BFloat16.h>
#include <base/types.h>
#include <llvm/IR/Value.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH;
}

void SerializationArrayT::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & arrayt = field.safeGet<const ArrayT &>();
    for (size_t element_index = 0; element_index < size; ++element_index)
        elems[element_index]->serializeBinary(arrayt[element_index], ostr, settings);
}

void SerializationArrayT::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    field = ArrayT();
    ArrayT & arrayt = field.safeGet<ArrayT &>();
    arrayt.reserve(size);
    for (size_t i = 0; i < size; ++i)
        elems[i]->deserializeBinary(arrayt.emplace_back(), istr, settings);
}

/* Get idx-th column */
static inline const IColumn & extractElementColumn(const IColumn & column, size_t idx)
{
    if (const auto * arrayt_column = typeid_cast<const ColumnArrayT *>(&column))
        return assert_cast<const ColumnTuple &>(arrayt_column->getTupleColumn()).getColumn(idx);
    return assert_cast<const ColumnTuple &>(column).getColumn(idx);
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
            auto & element_column = const_cast<IColumn &>(extractElementColumn(column, i));
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

        assert_cast<ColumnTuple &>(column).addSize(1);


        // Check that all columns now have the same size.
        size_t new_size = column.size();
        for (size_t i = 1; i < num_elems; ++i)
        {
            const auto & element_column = extractElementColumn(column, i);
            if (element_column.size() != new_size)
            {
                // This is not a logical error because it may work with user-supplied data.
                if constexpr (throw_exception)
                    throw Exception(
                        ErrorCodes::SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH, "Cannot read a tuple because not all elements are present");
                restore_elements();
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

void SerializationArrayT::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    for (size_t element_index = 0; element_index < size; ++element_index)
        elems[element_index]->serializeBinary(extractElementColumn(column, element_index), row_num, ostr, settings);
}

void SerializationArrayT::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    addElementSafe<void>(
        size,
        column,
        [&]() -> bool
        {
            for (size_t i = 0; i < size; ++i)
            {
                auto & element_column = const_cast<IColumn &>(extractElementColumn(column, i));
                elems[i]->deserializeBinary(element_column, istr, settings);
            }
            return true;
        });
}

template <typename FloatType>
void SerializationArrayT::readFloatsAndExtractBytes(ReadBuffer & istr, std::vector<char> & value_bytes) const
{
    FloatType value;
    const size_t bytes_per_float = size / n;

    for (size_t i = 0; i < n; ++i)
    {
        if (i != 0)
        {
            skipWhitespaceIfAny(istr);
            assertChar(',', istr);
            skipWhitespaceIfAny(istr);
        }

        readText(value, istr);

        const char * bytes = reinterpret_cast<const char *>(&value);

        for (size_t byte_index = 0; byte_index < bytes_per_float; ++byte_index)
        {
            size_t target_index = i * bytes_per_float + (bytes_per_float - 1 - byte_index);
            value_bytes[target_index] = bytes[byte_index];
        }
    }
}

void SerializationArrayT::serializeText(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & /*settings*/) const
{
    writeChar('[', ostr);

    size_t bytes_per_fixedstring = n >> 3;

    std::vector<char> value_bytes(size * bytes_per_fixedstring);

    const auto & column_arrayt = assert_cast<const ColumnArrayT &>(column);
    const auto & tuple_column = column_arrayt.getTupleColumn();

    for (size_t i = 0; i < size; ++i)
    {
        const auto & fixed_string_column = assert_cast<const ColumnFixedString &>(extractElementColumn(tuple_column, i));
        StringRef raw_data = fixed_string_column.getDataAt(row_num);

        for (size_t j = 0; j < bytes_per_fixedstring; ++j)
            value_bytes[i * bytes_per_fixedstring + j] = raw_data.data[j];
    }

    for (size_t i = 0; i < n; ++i)
    {
        if (i != 0)
            writeChar(',', ostr);

        size_t offset = i * (size / n);
        const unsigned char * bytes = reinterpret_cast<const unsigned char *>(value_bytes.data() + offset);

        switch (size)
        {
            case 16: {
                uint16_t bits = (static_cast<uint16_t>(bytes[0]) << 8) | static_cast<uint16_t>(bytes[1]);
                BFloat16 val;
                memcpy(&val, &bits, 2);
                writeText(val, ostr);
                break;
            }
            case 32: {
                uint32_t bits = (static_cast<uint32_t>(bytes[0]) << 24) | (static_cast<uint32_t>(bytes[1]) << 16)
                    | (static_cast<uint32_t>(bytes[2]) << 8) | static_cast<uint32_t>(bytes[3]);

                Float32 val;
                memcpy(&val, &bits, 4);

                writeText(val, ostr);
                break;
            }
            case 64: {
                uint64_t bits = (static_cast<uint64_t>(bytes[0]) << 56) | (static_cast<uint64_t>(bytes[1]) << 48)
                    | (static_cast<uint64_t>(bytes[2]) << 40) | (static_cast<uint64_t>(bytes[3]) << 32)
                    | (static_cast<uint64_t>(bytes[4]) << 24) | (static_cast<uint64_t>(bytes[5]) << 16)
                    | (static_cast<uint64_t>(bytes[6]) << 8) | static_cast<uint64_t>(bytes[7]);

                Float64 val;
                memcpy(&val, &bits, 8);

                writeText(val, ostr);
                break;
            }
            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported size for ArrayT: {}. Only 16, 32, and 64 are supported", size);
        }
    }


    writeChar(']', ostr);
}

void SerializationArrayT::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    auto & column_arrayt = assert_cast<ColumnArrayT &>(column);
    auto * tuple_column = column_arrayt.getTupleColumn();

    addElementSafe<void>(
        size,
        *tuple_column,
        [&]() -> bool
        {
            assertChar('[', istr);

            size_t bytes_per_fixedstring = n >> 3;

            std::vector<char> value_bytes(bytes_per_fixedstring * size);

            switch (size)
            {
                case 16:
                    readFloatsAndExtractBytes<BFloat16>(istr, value_bytes);
                    break;
                case 32:
                    readFloatsAndExtractBytes<Float32>(istr, value_bytes);
                    break;
                case 64:
                    readFloatsAndExtractBytes<Float64>(istr, value_bytes);
                    break;
                default:
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported size for ArrayT: {}. Only 16, 32, and 64 are supported", size);
            }

            skipWhitespaceIfAny(istr);
            assertChar(']', istr);

            std::vector<std::string> column_data;
            column_data.reserve(size);
            for (size_t i = 0; i < size; ++i)
                column_data.emplace_back(value_bytes.data() + i * bytes_per_fixedstring, bytes_per_fixedstring);

            for (size_t col_idx = 0; col_idx < size; ++col_idx)
            {
                auto & element_column = const_cast<IColumn &>(extractElementColumn(*tuple_column, col_idx));
                auto & fixed_string_column = assert_cast<ColumnFixedString &>(element_column);
                fixed_string_column.insertData(column_data[col_idx].data(), bytes_per_fixedstring);
            }

            if (whole && !istr.eof())
                throwUnexpectedDataAfterParsedValue(column, istr, settings, "ArrayT");

            return true;
        });
}

}
