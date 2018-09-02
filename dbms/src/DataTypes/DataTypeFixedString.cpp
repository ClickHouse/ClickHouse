#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>

#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeFactory.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}


std::string DataTypeFixedString::getName() const
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
    ostr.write(reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]), n);
}


void DataTypeFixedString::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnFixedString::Chars_t & data = static_cast<ColumnFixedString &>(column).getChars();
    size_t old_size = data.size();
    data.resize(old_size + n);
    try
    {
        istr.readStrict(reinterpret_cast<char *>(&data[old_size]), n);
    }
    catch (...)
    {
        data.resize_assume_reserved(old_size);
        throw;
    }
}


void DataTypeFixedString::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnFixedString::Chars_t & data = typeid_cast<const ColumnFixedString &>(column).getChars();

    size_t size = data.size() / n;

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&data[n * offset]), n * limit);
}


void DataTypeFixedString::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    ColumnFixedString::Chars_t & data = typeid_cast<ColumnFixedString &>(column).getChars();

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
    writeString(reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]), n, ostr);
}


void DataTypeFixedString::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeAnyEscapedString<'\''>(pos, pos + n, ostr);
}


template <typename Reader>
static inline void read(const DataTypeFixedString & self, IColumn & column, Reader && reader)
{
    ColumnFixedString::Chars_t & data = typeid_cast<ColumnFixedString &>(column).getChars();
    size_t prev_size = data.size();

    try
    {
        reader(data);
    }
    catch (...)
    {
        data.resize_assume_reserved(prev_size);
        throw;
    }

    if (data.size() < prev_size + self.getN())
        data.resize_fill(prev_size + self.getN());

    if (data.size() > prev_size + self.getN())
    {
        data.resize_assume_reserved(prev_size);
        throw Exception("Too large value for " + self.getName(), ErrorCodes::TOO_LARGE_STRING_SIZE);
    }
}


void DataTypeFixedString::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars_t & data) { readEscapedStringInto(data, istr); });
}


void DataTypeFixedString::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeAnyQuotedString<'\''>(pos, pos + n, ostr);
}


void DataTypeFixedString::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars_t & data) { readQuotedStringInto<true>(data, istr); });
}


void DataTypeFixedString::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const char * pos = reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeJSONString(pos, pos + n, ostr, settings);
}


void DataTypeFixedString::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars_t & data) { readJSONStringInto(data, istr); });
}


void DataTypeFixedString::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeXMLString(pos, pos + n, ostr);
}


void DataTypeFixedString::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeCSVString(pos, pos + n, ostr);
}


void DataTypeFixedString::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read(*this, column, [&istr, &csv = settings.csv](ColumnFixedString::Chars_t & data) { readCSVStringInto(data, istr, csv); });
}


MutableColumnPtr DataTypeFixedString::createColumn() const
{
    return ColumnFixedString::create(n);
}


bool DataTypeFixedString::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && n == static_cast<const DataTypeFixedString &>(rhs).n;
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("FixedString data type family must have exactly one argument - size in bytes", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ASTLiteral * argument = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
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
