#include <Poco/SharedPtr.h>

#include <DB/Common/VarInt.h>
#include <DB/Common/QuoteManipulators.h>
#include <DB/Common/EscapeManipulators.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataTypes/DataTypeString.h>


namespace DB
{

using Poco::SharedPtr;


void DataTypeString::serializeBinary(const Field & field, std::ostream & ostr) const
{
	const String & s = boost::get<String>(field);
	writeVarUInt(s.size(), ostr);
	ostr << s;
}


void DataTypeString::deserializeBinary(Field & field, std::istream & istr) const
{
	UInt64 size;
	readVarUInt(size, istr);
	if (!istr.good())
		return;
	field = String("");
	String & s = boost::get<String>(field);
	s.resize(size);
	/// непереносимо, но (действительно) быстрее
	istr.read(const_cast<char*>(s.data()), size);
}


void DataTypeString::serializeBinary(const IColumn & column, std::ostream & ostr) const
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


void DataTypeString::deserializeBinary(IColumn & column, std::istream & istr, size_t limit) const
{
	ColumnArray & column_array = dynamic_cast<ColumnArray &>(column);
	ColumnUInt8::Container_t & data = dynamic_cast<ColumnUInt8 &>(column_array.getData()).getData();
	ColumnArray::Offsets_t & offsets = column_array.getOffsets();

	data.reserve(limit);
	offsets.reserve(limit);

	size_t offset = 0;
	for (size_t i = 0; i < limit; ++i)
	{
		UInt64 size;
		readVarUInt(size, istr);

		if (!istr.good())
			break;
		
		offset += size;
		offsets.push_back(offset);

		if (data.size() < offset)
			data.resize(offset);
		
		istr.read(reinterpret_cast<char*>(&data[offset - size]), sizeof(ColumnUInt8::value_type) * size);

		if (!istr.good())
			throw Exception("Cannot read all data from stream", ErrorCodes::CANNOT_READ_DATA_FROM_ISTREAM);
	}
}


void DataTypeString::serializeText(const Field & field, std::ostream & ostr) const
{
	ostr << boost::get<const String &>(field);
}


void DataTypeString::deserializeText(Field & field, std::istream & istr) const
{
	istr >> boost::get<String &>(field);
}


void DataTypeString::serializeTextEscaped(const Field & field, std::ostream & ostr) const
{
	ostr << strconvert::escape_file << boost::get<const String &>(field);
}


void DataTypeString::deserializeTextEscaped(Field & field, std::istream & istr) const
{
	istr >> strconvert::unescape_file >> boost::get<String &>(field);
}


void DataTypeString::serializeTextQuoted(const Field & field, std::ostream & ostr, bool compatible) const
{
	ostr << strconvert::quote_fast << boost::get<const String &>(field);
}


void DataTypeString::deserializeTextQuoted(Field & field, std::istream & istr, bool compatible) const
{
	istr >> strconvert::unquote_fast >> boost::get<String &>(field);
}


SharedPtr<IColumn> DataTypeString::createColumn() const
{
	return new ColumnString;
}

}
