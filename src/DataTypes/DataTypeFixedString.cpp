#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>

#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeFactory.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}


std::string DataTypeFixedString::doGetName() const
{
    return "FixedString(" + toString(n) + ")";
}


void DataTypeFixedString::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const String & s = get<const String &>(field);
    ostr.write(s.data(), std::min(s.size(), n));
    if (s.size() < n)
        for (size_t i = s.size(); i < n; ++i)
            ostr.write(0);
}


void DataTypeFixedString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    field = String();
    String & s = get<String &>(field);
    s.resize(n);
    istr.readStrict(s.data(), n);
}


void DataTypeFixedString::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    ostr.write(reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]), n);
}


void DataTypeFixedString::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnFixedString::Chars & data = assert_cast<ColumnFixedString &>(column).getChars();
    size_t old_size = data.size();
    data.resize(old_size + n);
    try
    {
        istr.readStrict(reinterpret_cast<char *>(data.data() + old_size), n);
    }
    catch (...)
    {
        data.resize_assume_reserved(old_size);
        throw;
    }
}


void DataTypeFixedString::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnFixedString::Chars & data = typeid_cast<const ColumnFixedString &>(column).getChars();

    size_t size = data.size() / n;

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&data[n * offset]), n * limit);
}


void DataTypeFixedString::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    ColumnFixedString::Chars & data = typeid_cast<ColumnFixedString &>(column).getChars();

    size_t initial_size = data.size();
    size_t max_bytes = limit * n;
    data.resize(initial_size + max_bytes);
    size_t read_bytes = istr.readBig(reinterpret_cast<char *>(&data[initial_size]), max_bytes);

    if (read_bytes % n != 0)
        throw Exception("Cannot read all data of type FixedString. Bytes read:" + toString(read_bytes) + ". String size:" + toString(n) + ".",
            ErrorCodes::CANNOT_READ_ALL_DATA);

    data.resize(initial_size + read_bytes);
}


void DataTypeFixedString::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]), n, ostr);
}


void DataTypeFixedString::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeAnyEscapedString<'\''>(pos, pos + n, ostr);
}


static inline void alignStringLength(const DataTypeFixedString & type,
                                     ColumnFixedString::Chars & data,
                                     size_t string_start)
{
    size_t length = data.size() - string_start;
    if (length < type.getN())
    {
        data.resize_fill(string_start + type.getN());
    }
    else if (length > type.getN())
    {
        data.resize_assume_reserved(string_start);
        throw Exception("Too large value for " + type.getName(), ErrorCodes::TOO_LARGE_STRING_SIZE);
    }
}

template <typename Reader>
static inline void read(const DataTypeFixedString & self, IColumn & column, Reader && reader)
{
    ColumnFixedString::Chars & data = typeid_cast<ColumnFixedString &>(column).getChars();
    size_t prev_size = data.size();
    try
    {
        reader(data);
        alignStringLength(self, data, prev_size);
    }
    catch (...)
    {
        data.resize_assume_reserved(prev_size);
        throw;
    }
}


void DataTypeFixedString::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars & data) { readEscapedStringInto(data, istr); });
}


void DataTypeFixedString::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeAnyQuotedString<'\''>(pos, pos + n, ostr);
}


void DataTypeFixedString::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars & data) { readQuotedStringInto<true>(data, istr); });
}


void DataTypeFixedString::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars & data) { readStringInto(data, istr); });
}


void DataTypeFixedString::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeJSONString(pos, pos + n, ostr, settings);
}


void DataTypeFixedString::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars & data) { readJSONStringInto(data, istr); });
}


void DataTypeFixedString::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeXMLString(pos, pos + n, ostr);
}


void DataTypeFixedString::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeCSVString(pos, pos + n, ostr);
}


void DataTypeFixedString::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read(*this, column, [&istr, &csv = settings.csv](ColumnFixedString::Chars & data) { readCSVStringInto(data, istr, csv); });
}


void DataTypeFixedString::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    if (value_index)
        return;
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    value_index = static_cast<bool>(protobuf.writeString(StringRef(pos, n)));
}


void DataTypeFixedString::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    auto & column_string = assert_cast<ColumnFixedString &>(column);
    ColumnFixedString::Chars & data = column_string.getChars();
    size_t old_size = data.size();
    try
    {
        if (allow_add_row)
        {
            if (protobuf.readStringInto(data))
            {
                alignStringLength(*this, data, old_size);
                row_added = true;
            }
            else
                data.resize_assume_reserved(old_size);
        }
        else
        {
            ColumnFixedString::Chars temp_data;
            if (protobuf.readStringInto(temp_data))
            {
                alignStringLength(*this, temp_data, 0);
                column_string.popBack(1);
                old_size = data.size();
                data.insertSmallAllowReadWriteOverflow15(temp_data.begin(), temp_data.end());
            }
        }
    }
    catch (...)
    {
        data.resize_assume_reserved(old_size);
        throw;
    }
}


MutableColumnPtr DataTypeFixedString::createColumn() const
{
    return ColumnFixedString::create(n);
}

Field DataTypeFixedString::getDefault() const
{
    return String();
}

bool DataTypeFixedString::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && n == static_cast<const DataTypeFixedString &>(rhs).n;
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("FixedString data type family must have exactly one argument - size in bytes", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * argument = arguments->children[0]->as<ASTLiteral>();
    if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.get<UInt64>() == 0)
        throw Exception("FixedString data type family must have a number (positive integer) as its argument", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    return std::make_shared<DataTypeFixedString>(argument->value.get<UInt64>());
}


void registerDataTypeFixedString(DataTypeFactory & factory)
{
    factory.registerDataType("FixedString", create);

    /// Compatibility alias.
    factory.registerAlias("BINARY", "FixedString", DataTypeFactory::CaseInsensitive);
}

}
