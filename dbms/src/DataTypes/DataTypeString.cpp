#include <Poco/SharedPtr.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataTypes/DataTypeString.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/VarInt.h>


namespace DB
{

using Poco::SharedPtr;


void DataTypeString::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	const String & s = boost::get<String>(field);
	writeVarUInt(s.size(), ostr);
	writeString(s, ostr);
}


void DataTypeString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	UInt64 size;
	readVarUInt(size, istr);
	field = String("");
	String & s = boost::get<String>(field);
	s.resize(size);
	/// непереносимо, но (действительно) быстрее
	istr.readStrict(const_cast<char*>(s.data()), size);
}


void DataTypeString::serializeBinary(const IColumn & column, WriteBuffer & ostr) const
{
	const ColumnArray & column_array = dynamic_cast<const ColumnArray &>(column);
	const ColumnUInt8::Container_t & data = dynamic_cast<const ColumnUInt8 &>(column_array.getData()).getData();
	const ColumnArray::Offsets_t & offsets = column_array.getOffsets();

	size_t size = column_array.size();
	if (!size)
		return;

	writeVarUInt(offsets[0] - 1, ostr);
	ostr.write(reinterpret_cast<const char *>(&data[0]), offsets[0] - 1);
	
	for (size_t i = 1; i < size; ++i)
	{
		UInt64 str_size = offsets[i] - offsets[i - 1] - 1;
		writeVarUInt(str_size, ostr);
		ostr.write(reinterpret_cast<const char *>(&data[offsets[i - 1]]), str_size);
	}
}


void DataTypeString::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const
{
	ColumnArray & column_array = dynamic_cast<ColumnArray &>(column);
	ColumnUInt8::Container_t & data = dynamic_cast<ColumnUInt8 &>(column_array.getData()).getData();
	ColumnArray::Offsets_t & offsets = column_array.getOffsets();

	data.reserve(limit);
	offsets.reserve(limit);

	size_t offset = 0;
	for (size_t i = 0; i < limit; ++i)
	{
		if (istr.eof())
			break;

		UInt64 size;
		readVarUInt(size, istr);

		offset += size;
		offsets.push_back(offset);

		if (data.size() < offset)
			data.resize(offset);
		
		istr.readStrict(reinterpret_cast<char*>(&data[offset - size]), sizeof(ColumnUInt8::value_type) * size);
		data[offset - 1] = 0;
	}
}


void DataTypeString::serializeText(const Field & field, WriteBuffer & ostr) const
{
	writeString(boost::get<const String &>(field), ostr);
}


void DataTypeString::deserializeText(Field & field, ReadBuffer & istr) const
{
	String s;
	readString(s, istr);
	field = s;
}


void DataTypeString::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	writeEscapedString(boost::get<const String &>(field), ostr);
}


void DataTypeString::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	String s;
	readEscapedString(s, istr);
	field = s;
}


void DataTypeString::serializeTextQuoted(const Field & field, WriteBuffer & ostr, bool compatible) const
{
	writeQuotedString(boost::get<const String &>(field), ostr);
}


void DataTypeString::deserializeTextQuoted(Field & field, ReadBuffer & istr, bool compatible) const
{
	String s;
	readQuotedString(s, istr);
	field = s;
}


SharedPtr<IColumn> DataTypeString::createColumn() const
{
	return new ColumnString;
}

}
