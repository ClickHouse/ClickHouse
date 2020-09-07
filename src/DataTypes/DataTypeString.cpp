#include <Core/Defines.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <Core/Field.h>

#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFactory.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif


namespace DB
{


namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}


void DataTypeString::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const String & s = get<const String &>(field);
    writeVarUInt(s.size(), ostr);
    writeString(s, ostr);
}


void DataTypeString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    UInt64 size;
    readVarUInt(size, istr);
    field = String();
    String & s = get<String &>(field);
    s.resize(size);
    istr.readStrict(s.data(), size);
}


void DataTypeString::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const StringRef & s = assert_cast<const ColumnString &>(column).getDataAt(row_num);
    writeVarUInt(s.size, ostr);
    writeString(s, ostr);
}


void DataTypeString::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    UInt64 size;
    readVarUInt(size, istr);

    size_t old_chars_size = data.size();
    size_t offset = old_chars_size + size + 1;
    offsets.push_back(offset);

    try
    {
        data.resize(offset);
        istr.readStrict(reinterpret_cast<char*>(&data[offset - size - 1]), size);
        data.back() = 0;
    }
    catch (...)
    {
        offsets.pop_back();
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}


void DataTypeString::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnString & column_string = typeid_cast<const ColumnString &>(column);
    const ColumnString::Chars & data = column_string.getChars();
    const ColumnString::Offsets & offsets = column_string.getOffsets();

    size_t size = column.size();
    if (!size)
        return;

    size_t end = limit && offset + limit < size
        ? offset + limit
        : size;

    if (offset == 0)
    {
        UInt64 str_size = offsets[0] - 1;
        writeVarUInt(str_size, ostr);
        ostr.write(reinterpret_cast<const char *>(data.data()), str_size);

        ++offset;
    }

    for (size_t i = offset; i < end; ++i)
    {
        UInt64 str_size = offsets[i] - offsets[i - 1] - 1;
        writeVarUInt(str_size, ostr);
        ostr.write(reinterpret_cast<const char *>(&data[offsets[i - 1]]), str_size);
    }
}


template <int UNROLL_TIMES>
static NO_INLINE void deserializeBinarySSE2(ColumnString::Chars & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit)
{
    size_t offset = data.size();
    for (size_t i = 0; i < limit; ++i)
    {
        if (istr.eof())
            break;

        UInt64 size;
        readVarUInt(size, istr);

        offset += size + 1;
        offsets.push_back(offset);

        data.resize(offset);

        if (size)
        {
#ifdef __SSE2__
            /// An optimistic branch in which more efficient copying is possible.
            if (offset + 16 * UNROLL_TIMES <= data.capacity() && istr.position() + size + 16 * UNROLL_TIMES <= istr.buffer().end())
            {
                const __m128i * sse_src_pos = reinterpret_cast<const __m128i *>(istr.position());
                const __m128i * sse_src_end = sse_src_pos + (size + (16 * UNROLL_TIMES - 1)) / 16 / UNROLL_TIMES * UNROLL_TIMES;
                __m128i * sse_dst_pos = reinterpret_cast<__m128i *>(&data[offset - size - 1]);

                while (sse_src_pos < sse_src_end)
                {
                    for (size_t j = 0; j < UNROLL_TIMES; ++j)
                        _mm_storeu_si128(sse_dst_pos + j, _mm_loadu_si128(sse_src_pos + j));

                    sse_src_pos += UNROLL_TIMES;
                    sse_dst_pos += UNROLL_TIMES;
                }

                istr.position() += size;
            }
            else
#endif
            {
                istr.readStrict(reinterpret_cast<char*>(&data[offset - size - 1]), size);
            }
        }

        data[offset - 1] = 0;
    }
}


void DataTypeString::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    ColumnString & column_string = typeid_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    double avg_chars_size = 1; /// By default reserve only for empty strings.

    if (avg_value_size_hint && avg_value_size_hint > sizeof(offsets[0]))
    {
        /// Randomly selected.
        constexpr auto avg_value_size_hint_reserve_multiplier = 1.2;

        avg_chars_size = (avg_value_size_hint - sizeof(offsets[0])) * avg_value_size_hint_reserve_multiplier;
    }

    size_t size_to_reserve = data.size() + std::ceil(limit * avg_chars_size);

    /// Never reserve for too big size.
    if (size_to_reserve < 256 * 1024 * 1024)
    {
        try
        {
            data.reserve(size_to_reserve);
        }
        catch (Exception & e)
        {
            e.addMessage(
                "(avg_value_size_hint = " + toString(avg_value_size_hint)
                + ", avg_chars_size = " + toString(avg_chars_size)
                + ", limit = " + toString(limit) + ")");
            throw;
        }
    }

    offsets.reserve(offsets.size() + limit);

    if (avg_chars_size >= 64)
        deserializeBinarySSE2<4>(data, offsets, istr, limit);
    else if (avg_chars_size >= 48)
        deserializeBinarySSE2<3>(data, offsets, istr, limit);
    else if (avg_chars_size >= 32)
        deserializeBinarySSE2<2>(data, offsets, istr, limit);
    else
        deserializeBinarySSE2<1>(data, offsets, istr, limit);
}


void DataTypeString::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void DataTypeString::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


template <typename Reader>
static inline void read(IColumn & column, Reader && reader)
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try
    {
        reader(data);
        data.push_back(0);
        offsets.push_back(data.size());
    }
    catch (...)
    {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}


void DataTypeString::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readStringInto(data, istr); });
}


void DataTypeString::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readEscapedStringInto(data, istr); });
}


void DataTypeString::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeQuotedString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void DataTypeString::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readQuotedStringInto<true>(data, istr); });
}


void DataTypeString::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr, settings);
}


void DataTypeString::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(column, [&](ColumnString::Chars & data) { readJSONStringInto(data, istr); });
}


void DataTypeString::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeXMLString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void DataTypeString::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSVString<>(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void DataTypeString::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read(column, [&](ColumnString::Chars & data) { readCSVStringInto(data, istr, settings.csv); });
}


void DataTypeString::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    if (value_index)
        return;
    value_index = static_cast<bool>(protobuf.writeString(assert_cast<const ColumnString &>(column).getDataAt(row_num)));
}


void DataTypeString::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    auto & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();
    size_t old_size = offsets.size();
    try
    {
        if (allow_add_row)
        {
            if (protobuf.readStringInto(data))
            {
                data.emplace_back(0);
                offsets.emplace_back(data.size());
                row_added = true;
            }
            else
                data.resize_assume_reserved(offsets.back());
        }
        else
        {
            ColumnString::Chars temp_data;
            if (protobuf.readStringInto(temp_data))
            {
                temp_data.emplace_back(0);
                column_string.popBack(1);
                old_size = offsets.size();
                data.insertSmallAllowReadWriteOverflow15(temp_data.begin(), temp_data.end());
                offsets.emplace_back(data.size());
            }
        }
    }
    catch (...)
    {
        offsets.resize_assume_reserved(old_size);
        data.resize_assume_reserved(offsets.back());
        throw;
    }
}

Field DataTypeString::getDefault() const
{
    return String();
}

MutableColumnPtr DataTypeString::createColumn() const
{
    return ColumnString::create();
}


bool DataTypeString::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (arguments && !arguments->children.empty())
    {
        if (arguments->children.size() > 1)
            throw Exception("String data type family mustn't have more than one argument - size in characters", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * argument = arguments->children[0]->as<ASTLiteral>();
        if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.get<UInt64>() == 0)
            throw Exception("String data type family may have only a number (positive integer) as its argument", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    }

    return std::make_shared<DataTypeString>();
}


void registerDataTypeString(DataTypeFactory & factory)
{
    factory.registerDataType("String", create);

    /// These synonims are added for compatibility.

    factory.registerAlias("CHAR", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NCHAR", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("CHARACTER", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("VARCHAR", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NVARCHAR", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("VARCHAR2", "String", DataTypeFactory::CaseInsensitive); /// Oracle
    factory.registerAlias("TEXT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("TINYTEXT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("MEDIUMTEXT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("LONGTEXT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BLOB", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("CLOB", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("TINYBLOB", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("MEDIUMBLOB", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("LONGBLOB", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BYTEA", "String", DataTypeFactory::CaseInsensitive); /// PostgreSQL

    factory.registerAlias("CHARACTER LARGE OBJECT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("CHARACTER VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("CHAR LARGE OBJECT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("CHAR VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NATIONAL CHAR", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NATIONAL CHARACTER", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NATIONAL CHARACTER LARGE OBJECT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NATIONAL CHARACTER VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NATIONAL CHAR VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NCHAR VARYING", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("NCHAR LARGE OBJECT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BINARY LARGE OBJECT", "String", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("BINARY VARYING", "String", DataTypeFactory::CaseInsensitive);
}
}
