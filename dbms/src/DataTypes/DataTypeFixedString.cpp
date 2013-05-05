#include <Poco/SharedPtr.h>

#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnConst.h>

#include <DB/DataTypes/DataTypeFixedString.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/VarInt.h>


namespace DB
{

using Poco::SharedPtr;


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
	/// непереносимо, но (действительно) быстрее
	istr.readStrict(&s[0], n);
}


void DataTypeFixedString::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnFixedString::Chars_t & data = dynamic_cast<const ColumnFixedString &>(column).getChars();

	size_t size = data.size() / n;

	if (limit == 0 || offset + limit > size)
		limit = size - offset;

	ostr.write(reinterpret_cast<const char *>(&data[offset]), n * limit);
}


void DataTypeFixedString::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const
{
	ColumnFixedString::Chars_t & data = dynamic_cast<ColumnFixedString &>(column).getChars();

	size_t max_bytes = limit * n;
	data.resize(max_bytes);
	size_t read_bytes = istr.read(reinterpret_cast<char *>(&data[0]), max_bytes);

	if (read_bytes % n != 0)
		throw Exception("Cannot read all data of type FixedString",
			ErrorCodes::CANNOT_READ_ALL_DATA);

	data.resize(read_bytes);
}


void DataTypeFixedString::serializeText(const Field & field, WriteBuffer & ostr) const
{
	writeString(get<const String &>(field), ostr);
}


void DataTypeFixedString::deserializeText(Field & field, ReadBuffer & istr) const
{
	field.assignString("", 0);
	readString(get<String &>(field), istr);
}


void DataTypeFixedString::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	writeEscapedString(get<const String &>(field), ostr);
}


void DataTypeFixedString::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	field.assignString("", 0);
	readEscapedString(get<String &>(field), istr);
}


void DataTypeFixedString::serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
{
	writeQuotedString(get<const String &>(field), ostr);
}


void DataTypeFixedString::deserializeTextQuoted(Field & field, ReadBuffer & istr) const
{
	field.assignString("", 0);
	readQuotedString(get<String &>(field), istr);
}


ColumnPtr DataTypeFixedString::createColumn() const
{
	return new ColumnFixedString(n);
}


ColumnPtr DataTypeFixedString::createConstColumn(size_t size, const Field & field) const
{
	return new ColumnConstString(size, get<const String &>(field), clone());
}

}
