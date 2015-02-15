#include <Poco/SharedPtr.h>

#include <DB/Core/Defines.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnConst.h>

#include <DB/DataTypes/DataTypeString.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/VarInt.h>


namespace DB
{

using Poco::SharedPtr;


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
	istr.readStrict(&s[0], size);
}


void DataTypeString::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnString & column_string = typeid_cast<const ColumnString &>(column);
	const ColumnString::Chars_t & data = column_string.getChars();
	const ColumnString::Offsets_t & offsets = column_string.getOffsets();

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
		ostr.write(reinterpret_cast<const char *>(&data[0]), str_size);

		++offset;
	}

	for (size_t i = offset; i < end; ++i)
	{
		UInt64 str_size = offsets[i] - offsets[i - 1] - 1;
		writeVarUInt(str_size, ostr);
		ostr.write(reinterpret_cast<const char *>(&data[offsets[i - 1]]), str_size);
	}
}


void DataTypeString::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnString & column_string = typeid_cast<ColumnString &>(column);
	ColumnString::Chars_t & data = column_string.getChars();
	ColumnString::Offsets_t & offsets = column_string.getOffsets();

	/// Выбрано наугад.
	constexpr auto avg_value_size_hint_reserve_multiplier = 1.2;

	data.reserve(data.size()
		+ std::ceil(limit * (avg_value_size_hint && avg_value_size_hint > sizeof(offsets[0])
			? (avg_value_size_hint - sizeof(offsets[0])) * avg_value_size_hint_reserve_multiplier
			: DBMS_APPROX_STRING_SIZE)));

	offsets.reserve(offsets.size() + limit);

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

		istr.readStrict(reinterpret_cast<char*>(&data[offset - size - 1]), sizeof(ColumnUInt8::value_type) * size);
		data[offset - 1] = 0;
	}
}


void DataTypeString::serializeText(const Field & field, WriteBuffer & ostr) const
{
	writeString(get<const String &>(field), ostr);
}


void DataTypeString::deserializeText(Field & field, ReadBuffer & istr) const
{
	field.assignString("", 0);
	readString(get<String &>(field), istr);
}


void DataTypeString::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	writeEscapedString(get<const String &>(field), ostr);
}


void DataTypeString::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	field.assignString("", 0);
	readEscapedString(get<String &>(field), istr);
}


void DataTypeString::serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
{
	writeQuotedString(get<const String &>(field), ostr);
}


void DataTypeString::deserializeTextQuoted(Field & field, ReadBuffer & istr) const
{
	field.assignString("", 0);
	readQuotedString(get<String &>(field), istr);
}


void DataTypeString::serializeTextJSON(const Field & field, WriteBuffer & ostr) const
{
	writeJSONString(get<const String &>(field), ostr);
}


ColumnPtr DataTypeString::createColumn() const
{
	return new ColumnString;
}


ColumnPtr DataTypeString::createConstColumn(size_t size, const Field & field) const
{
	return new ColumnConst<String>(size, get<const String &>(field));
}

}
