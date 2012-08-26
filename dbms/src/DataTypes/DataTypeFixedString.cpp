#include <Poco/SharedPtr.h>

#include <DB/Columns/ColumnFixedArray.h>
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
	const String & s = boost::get<String>(field);
	ostr.write(s.data(), std::min(s.size(), n));
	if (s.size() < n)
		for (size_t i = s.size(); i < n; ++i)
			ostr.write(0);
}


void DataTypeFixedString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	field = String();
	String & s = boost::get<String>(field);
	s.resize(n);
	/// непереносимо, но (действительно) быстрее
	istr.readStrict(const_cast<char*>(s.data()), n);
}


void DataTypeFixedString::serializeBinary(const IColumn & column, WriteBuffer & ostr, WriteCallback callback) const
{
	const ColumnFixedArray & column_array = dynamic_cast<const ColumnFixedArray &>(column);
	const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_array.getData()).getData();

	size_t prev_callback_point = 0;
	size_t next_callback_point = 0;
	size_t size = data.size() / n;

	while (next_callback_point < size)
	{
		next_callback_point = callback ? callback() : size;
		if (next_callback_point > size)
			next_callback_point = size;

		ostr.write(reinterpret_cast<const char *>(&data[prev_callback_point * n]),
			n * (next_callback_point - prev_callback_point));

		prev_callback_point = next_callback_point;
	}
}


void DataTypeFixedString::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const
{
	ColumnFixedArray & column_array = dynamic_cast<ColumnFixedArray &>(column);
	ColumnUInt8::Container_t & data = dynamic_cast<ColumnUInt8 &>(column_array.getData()).getData();

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
	writeString(boost::get<const String &>(field), ostr);
}


void DataTypeFixedString::deserializeText(Field & field, ReadBuffer & istr) const
{
	String s;
	readString(s, istr);
	field = s;
}


void DataTypeFixedString::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	writeEscapedString(boost::get<const String &>(field), ostr);
}


void DataTypeFixedString::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	String s;
	readEscapedString(s, istr);
	field = s;
}


void DataTypeFixedString::serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
{
	writeQuotedString(boost::get<const String &>(field), ostr);
}


void DataTypeFixedString::deserializeTextQuoted(Field & field, ReadBuffer & istr) const
{
	String s;
	readQuotedString(s, istr);
	field = s;
}


ColumnPtr DataTypeFixedString::createColumn() const
{
	return new ColumnFixedString(n);
}


ColumnPtr DataTypeFixedString::createConstColumn(size_t size, const Field & field) const
{
	return new ColumnConst<String>(size, boost::get<String>(field));
}

}
