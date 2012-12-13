#include <boost/bind.hpp>

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnConst.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>


namespace DB
{


DataTypeArray::DataTypeArray(DataTypePtr nested_) : nested(nested_)
{
	offsets = new DataTypeFromFieldType<ColumnArray::Offset_t>::Type;
}

	
void DataTypeArray::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	throw Exception("Binary serialization of individual array values is not implemented.", ErrorCodes::NOT_IMPLEMENTED);
}


void DataTypeArray::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	throw Exception("Binary serialization of individual array values is not implemented.", ErrorCodes::NOT_IMPLEMENTED);
}


void DataTypeArray::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnArray & column_array = dynamic_cast<const ColumnArray &>(column);
	const ColumnArray::Offsets_t & offsets = column_array.getOffsets();

	size_t nested_offset = offset ? offsets[offset] : 0;
	size_t nested_limit = limit && offset + limit < offsets.size()
		? offsets[offset + limit] - nested_offset
		: 0;

	nested->serializeBinary(column_array.getData(), ostr, nested_offset, nested_limit);
}


void DataTypeArray::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const
{
	ColumnArray & column_array = dynamic_cast<ColumnArray &>(column);
	ColumnArray::Offsets_t & offsets = column_array.getOffsets();

	/// Должно быть считано согласнованное с offsets количество значений.
	size_t nested_limit = offsets.empty() ? 0 : offsets.back();
	nested->deserializeBinary(column_array.getData(), istr, nested_limit);

	if (column_array.getData().size() != nested_limit)
		throw Exception("Cannot read all array values", ErrorCodes::CANNOT_READ_ALL_DATA);
}


void DataTypeArray::serializeOffsets(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnArray & column_array = dynamic_cast<const ColumnArray &>(column);
	const ColumnArray::Offsets_t & offsets = column_array.getOffsets();
	size_t size = offsets.size();

	if (!size)
		return;

	size_t end = limit && offset + limit < size
		? offset + limit
		: size;

	if (offset == 0)
	{
		writeIntBinary(offsets[0], ostr);
		++offset;
	}

	for (size_t i = offset; i < end; ++i)
		writeIntBinary(offsets[i] - offsets[i - 1], ostr);
}


void DataTypeArray::deserializeOffsets(IColumn & column, ReadBuffer & istr, size_t limit) const
{
	ColumnArray & column_array = dynamic_cast<ColumnArray &>(column);
	ColumnArray::Offsets_t & offsets = column_array.getOffsets();
	offsets.resize(limit);

	size_t i = 0;
	ColumnArray::Offset_t current_offset = 0;
	while (i < limit && !istr.eof())
	{
		ColumnArray::Offset_t current_size = 0;
		readIntBinary(current_size, istr);
		current_offset += current_size;
		offsets[i] = current_offset;
		++i;
	}

	offsets.resize(i);
}


void DataTypeArray::serializeText(const Field & field, WriteBuffer & ostr) const
{
	const Array & arr = boost::get<const Array &>(field);

	writeChar('[', ostr);
	for (size_t i = 0, size = arr.size(); i < size; ++i)
	{
		if (i != 0)
			writeChar(',', ostr);
		nested->serializeTextQuoted(arr[i], ostr);
	}
	writeChar(']', ostr);
}


void DataTypeArray::deserializeText(Field & field, ReadBuffer & istr) const
{
	Array arr;

	bool first = true;
	assertString("[", istr);
	while (!istr.eof() && *istr.position() != ']')
	{
		if (!first)
		{
			if (*istr.position() == ',')
				++istr.position();
			else
				throw Exception("Cannot read array from text", ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT);
		}

		first = false;

		arr.push_back(Field());
		nested->deserializeTextQuoted(arr.back(), istr);
	}
	assertString("]", istr);

	field = arr;
}


void DataTypeArray::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	serializeText(field, ostr);
}


void DataTypeArray::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	deserializeText(field, istr);
}


void DataTypeArray::serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
{
	serializeText(field, ostr);
}


void DataTypeArray::deserializeTextQuoted(Field & field, ReadBuffer & istr) const
{
	deserializeText(field, istr);
}


ColumnPtr DataTypeArray::createColumn() const
{
	return new ColumnArray(nested->createColumn());
}


ColumnPtr DataTypeArray::createConstColumn(size_t size, const Field & field) const
{
	return new ColumnConst<Array>(size, boost::get<Array>(field));
}

}
