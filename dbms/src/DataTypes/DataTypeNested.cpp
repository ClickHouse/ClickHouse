#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnNested.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/WriteBufferFromString.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/DataTypes/DataTypeNested.h>

#include <DB/DataTypes/DataTypeArray.h>


namespace DB
{


DataTypeNested::DataTypeNested(NamesAndTypesListPtr nested_) : nested(nested_)
{
	offsets = new DataTypeFromFieldType<ColumnArray::Offset_t>::Type;
}


std::string DataTypeNested::concatenateNestedName(const std::string & nested_table_name, const std::string & nested_field_name)
{
	return nested_table_name + "." + nested_field_name;
}


std::string DataTypeNested::extractNestedTableName(const std::string & nested_name)
{
	const char * first_pos = strchr(nested_name.data(), '.');
	const char * last_pos = strrchr(nested_name.data(), '.');
	if (first_pos != last_pos)
		throw Exception("Invalid nested column name: " + nested_name, ErrorCodes::INVALID_NESTED_NAME);
	return first_pos == nullptr ? nested_name : nested_name.substr(0, first_pos - nested_name.data());
}


std::string DataTypeNested::extractNestedColumnName(const std::string & nested_name)
{
	const char * first_pos = strchr(nested_name.data(), '.');
	const char * last_pos = strrchr(nested_name.data(), '.');
	if (first_pos != last_pos)
		throw Exception("Invalid nested column name: " + nested_name, ErrorCodes::INVALID_NESTED_NAME);
	return last_pos == nullptr ? nested_name : nested_name.substr(last_pos - nested_name.data() + 1);
}


std::string DataTypeNested::getName() const
{
	std::string res;
	WriteBufferFromString out(res);

	writeCString("Nested(", out);

	for (NamesAndTypesList::const_iterator it = nested->begin(); it != nested->end(); ++it)
	{
		if (it != nested->begin())
			writeCString(", ", out);
		writeString(it->name, out);
		writeChar(' ', out);
		writeString(it->type->getName(), out);
	}

	writeChar(')', out);

	return res;
}


void DataTypeNested::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	throw Exception("Method serializeBinary(const Field &, WriteBuffer &) is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}


void DataTypeNested::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	throw Exception("Method deserializeBinary(Field &, ReadBuffer &) is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}


void DataTypeNested::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnNested & column_nested = typeid_cast<const ColumnNested &>(column);
	const ColumnNested::Offsets_t & offsets = column_nested.getOffsets();

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
	{
		NamesAndTypesList::const_iterator it = nested->begin();
		for (size_t i = 0; i < nested->size(); ++i, ++it)
			it->type->serializeBinary(*column_nested.getData()[i], ostr, nested_offset, nested_limit);
	}
}


void DataTypeNested::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnNested & column_nested = typeid_cast<ColumnNested &>(column);
	ColumnNested::Offsets_t & offsets = column_nested.getOffsets();

	/// Должно быть считано согласованное с offsets количество значений.
	size_t last_offset = (offsets.empty() ? 0 : offsets.back());
	if (last_offset < column_nested.size())
		throw Exception("Nested column longer than last offset", ErrorCodes::LOGICAL_ERROR);
	size_t nested_limit = (offsets.empty() ? 0 : offsets.back()) - column_nested.size();

	NamesAndTypesList::const_iterator it = nested->begin();
	for (size_t i = 0; i < nested->size(); ++i, ++it)
	{
		it->type->deserializeBinary(*column_nested.getData()[i], istr, nested_limit, 0);
		if (column_nested.getData()[i]->size() != last_offset)
			throw Exception("Cannot read all nested column values", ErrorCodes::CANNOT_READ_ALL_DATA);
	}
}


void DataTypeNested::serializeOffsets(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnNested & column_nested = typeid_cast<const ColumnNested &>(column);
	const ColumnNested::Offsets_t & offsets = column_nested.getOffsets();
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


void DataTypeNested::deserializeOffsets(IColumn & column, ReadBuffer & istr, size_t limit) const
{
	ColumnNested & column_nested = typeid_cast<ColumnNested &>(column);
	ColumnNested::Offsets_t & offsets = column_nested.getOffsets();
	size_t initial_size = offsets.size();
	offsets.resize(initial_size + limit);

	size_t i = initial_size;
	ColumnNested::Offset_t current_offset = initial_size ? offsets[initial_size - 1] : 0;
	while (i < initial_size + limit && !istr.eof())
	{
		ColumnNested::Offset_t current_size = 0;
		readIntBinary(current_size, istr);
		current_offset += current_size;
		offsets[i] = current_offset;
		++i;
	}

	offsets.resize(i);
}


void DataTypeNested::serializeText(const Field & field, WriteBuffer & ostr) const
{
	throw Exception("Method get is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}


void DataTypeNested::deserializeText(Field & field, ReadBuffer & istr) const
{
	throw Exception("Method get is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}


void DataTypeNested::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	serializeText(field, ostr);
}


void DataTypeNested::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	deserializeText(field, istr);
}


void DataTypeNested::serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
{
	serializeText(field, ostr);
}


void DataTypeNested::deserializeTextQuoted(Field & field, ReadBuffer & istr) const
{
	deserializeText(field, istr);
}


void DataTypeNested::serializeTextJSON(const Field & field, WriteBuffer & ostr) const
{
	throw Exception("Method get is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}


ColumnPtr DataTypeNested::createColumn() const
{
	Columns columns;
	columns.reserve(nested->size());
	for (NamesAndTypesList::const_iterator it = nested->begin(); it != nested->end(); ++it)
		columns.push_back(it->type->createColumn());
	return new ColumnNested(columns);
}


ColumnPtr DataTypeNested::createConstColumn(size_t size, const Field & field) const
{
	throw Exception("Method createConstColumn is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

NamesAndTypesListPtr DataTypeNested::expandNestedColumns(const NamesAndTypesList & names_and_types)
{
	NamesAndTypesListPtr columns = new NamesAndTypesList;
	for (NamesAndTypesList::const_iterator it = names_and_types.begin(); it != names_and_types.end(); ++it)
	{
		if (const DataTypeNested * type_nested = typeid_cast<const DataTypeNested *>(&*it->type))
		{
			const NamesAndTypesList & nested = *type_nested->getNestedTypesList();
			for (NamesAndTypesList::const_iterator jt = nested.begin(); jt != nested.end(); ++jt)
			{
				String nested_name = DataTypeNested::concatenateNestedName(it->name, jt->name);
				columns->push_back(NameAndTypePair(nested_name, new DataTypeArray(jt->type)));
			}
		}
		else
			columns->push_back(*it);
	}
	return columns;
}
}
