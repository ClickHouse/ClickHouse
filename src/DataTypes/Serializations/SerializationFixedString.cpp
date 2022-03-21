#include <DataTypes/Serializations/SerializationFixedString.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>

#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
}

void SerializationFixedString::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const String & s = get<const String &>(field);
    ostr.write(s.data(), std::min(s.size(), n));
    if (s.size() < n)
        for (size_t i = s.size(); i < n; ++i)
            ostr.write(0);
}


void SerializationFixedString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    field = String();
    String & s = get<String &>(field);
    s.resize(n);
    istr.readStrict(s.data(), n);
}


void SerializationFixedString::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    ostr.write(reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]), n);
}


void SerializationFixedString::deserializeBinary(IColumn & column, ReadBuffer & istr) const
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


void SerializationFixedString::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnFixedString::Chars & data = typeid_cast<const ColumnFixedString &>(column).getChars();

    size_t size = data.size() / n;

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&data[n * offset]), n * limit);
}


void SerializationFixedString::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
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


void SerializationFixedString::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]), n, ostr);
}


void SerializationFixedString::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeAnyEscapedString<'\''>(pos, pos + n, ostr);
}


void SerializationFixedString::alignStringLength(size_t n, PaddedPODArray<UInt8> & data, size_t string_start)
{
    size_t length = data.size() - string_start;
    if (length < n)
    {
        data.resize_fill(string_start + n);
    }
    else if (length > n)
    {
        data.resize_assume_reserved(string_start);
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large value for FixedString({})", n);
    }
}

template <typename Reader>
static inline void read(const SerializationFixedString & self, IColumn & column, Reader && reader)
{
    ColumnFixedString::Chars & data = typeid_cast<ColumnFixedString &>(column).getChars();
    size_t prev_size = data.size();
    try
    {
        reader(data);
        SerializationFixedString::alignStringLength(self.getN(), data, prev_size);
    }
    catch (...)
    {
        data.resize_assume_reserved(prev_size);
        throw;
    }
}


void SerializationFixedString::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars & data) { readEscapedStringInto(data, istr); });
}


void SerializationFixedString::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeAnyQuotedString<'\''>(pos, pos + n, ostr);
}


void SerializationFixedString::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars & data) { readQuotedStringInto<true>(data, istr); });
}


void SerializationFixedString::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars & data) { readStringUntilEOFInto(data, istr); });
}


void SerializationFixedString::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeJSONString(pos, pos + n, ostr, settings);
}


void SerializationFixedString::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars & data) { readJSONStringInto(data, istr); });
}


void SerializationFixedString::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeXMLStringForTextElement(pos, pos + n, ostr);
}


void SerializationFixedString::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeCSVString(pos, pos + n, ostr);
}


void SerializationFixedString::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read(*this, column, [&istr, &csv = settings.csv](ColumnFixedString::Chars & data) { readCSVStringInto(data, istr, csv); });
}


}
