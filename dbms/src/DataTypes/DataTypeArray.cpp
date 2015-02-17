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
	const Array & a = get<const Array &>(field);
	writeVarUInt(a.size(), ostr);
	for (size_t i = 0; i < a.size(); ++i)
	{
		nested->serializeBinary(a[i], ostr);
	}
}


void DataTypeArray::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	size_t size;
	readVarUInt(size, istr);
	field = Array(size);
	Array & arr = get<Array &>(field);
	for (size_t i = 0; i < size; ++i)
		nested->deserializeBinary(arr[i], istr);
}


void DataTypeArray::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnArray & column_array = typeid_cast<const ColumnArray &>(column);
	const ColumnArray::Offsets_t & offsets = column_array.getOffsets();

	if (offset > offsets.size())
		return;

	/** offset - с какого массива писать.
	  * limit - сколько массивов максимум записать, или 0, если писать всё, что есть.
	  * end - до какого массива заканчивается записываемый кусок.
	  *
	  * nested_offset - с какого элемента внутренностей писать.
	  * nested_limit - сколько элементов внутренностей писать, или 0, если писать всё, что есть.
	  */

	size_t end = std::min(offset + limit, offsets.size());

	size_t nested_offset = offset ? offsets[offset - 1] : 0;
	size_t nested_limit = limit
		? offsets[end - 1] - nested_offset
		: 0;

	if (limit == 0 || nested_limit)
		nested->serializeBinary(column_array.getData(), ostr, nested_offset, nested_limit);
}


void DataTypeArray::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnArray & column_array = typeid_cast<ColumnArray &>(column);
	ColumnArray::Offsets_t & offsets = column_array.getOffsets();
	IColumn & nested_column = column_array.getData();

	/// Должно быть считано согласнованное с offsets количество значений.
	size_t last_offset = (offsets.empty() ? 0 : offsets.back());
	if (last_offset < nested_column.size())
		throw Exception("Nested column longer than last offset", ErrorCodes::LOGICAL_ERROR);
	size_t nested_limit = last_offset - nested_column.size();
	nested->deserializeBinary(nested_column, istr, nested_limit, 0);

	if (column_array.getData().size() != last_offset)
		throw Exception("Cannot read all array values", ErrorCodes::CANNOT_READ_ALL_DATA);
}


void DataTypeArray::serializeOffsets(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnArray & column_array = typeid_cast<const ColumnArray &>(column);
	const ColumnArray::Offsets_t & offsets = column_array.getOffsets();
	size_t size = offsets.size();

	if (!size)
		return;

	size_t end = limit && (offset + limit < size)
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
	ColumnArray & column_array = typeid_cast<ColumnArray &>(column);
	ColumnArray::Offsets_t & offsets = column_array.getOffsets();
	size_t initial_size = offsets.size();
	offsets.resize(initial_size + limit);

	size_t i = initial_size;
	ColumnArray::Offset_t current_offset = initial_size ? offsets[initial_size - 1] : 0;
	while (i < initial_size + limit && !istr.eof())
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
	const Array & arr = get<const Array &>(field);

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

		skipWhitespaceIfAny(istr);

		if (*istr.position() == ']')
			break;

		arr.push_back(Field());
		nested->deserializeTextQuoted(arr.back(), istr);

		skipWhitespaceIfAny(istr);
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


void DataTypeArray::serializeTextJSON(const Field & field, WriteBuffer & ostr) const
{
	const Array & arr = get<const Array &>(field);

	writeChar('[', ostr);
	for (size_t i = 0, size = arr.size(); i < size; ++i)
	{
		if (i != 0)
			writeChar(',', ostr);
		nested->serializeTextJSON(arr[i], ostr);
	}
	writeChar(']', ostr);
}


ColumnPtr DataTypeArray::createColumn() const
{
	return new ColumnArray(nested->createColumn());
}


ColumnPtr DataTypeArray::createConstColumn(size_t size, const Field & field) const
{
	/// Последним аргументом нельзя отдать this.
	return new ColumnConstArray(size, get<const Array &>(field), new DataTypeArray(nested));
}

}
