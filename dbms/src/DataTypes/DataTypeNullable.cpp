#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/Common/typeid_cast.h>
#include <DB/DataTypes/NullSymbol.h>

namespace DB
{

DataTypeNullable::DataTypeNullable(DataTypePtr nested_data_type_)
	: nested_data_type{nested_data_type_}
{
}

std::string DataTypeNullable::getName() const
{
	return "Nullable(" + nested_data_type.get()->getName() + ")";
}

bool DataTypeNullable::isNullable() const
{
	return true;
}

bool DataTypeNullable::isNumeric() const
{
	return nested_data_type.get()->isNumeric();
}

bool DataTypeNullable::behavesAsNumber() const
{
	return nested_data_type.get()->behavesAsNumber();
}

DataTypePtr DataTypeNullable::clone() const
{
	return std::make_shared<DataTypeNullable>(nested_data_type.get()->clone());
}

void DataTypeNullable::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type.get()->serializeBinary(*(col->getNestedColumn().get()), ostr, offset, limit);
}

void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type.get()->deserializeBinary(*(col->getNestedColumn().get()), istr, limit, avg_value_size_hint);
}

void DataTypeNullable::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	nested_data_type.get()->serializeBinary(field, ostr);
}

void DataTypeNullable::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	nested_data_type.get()->deserializeBinary(field, istr);
}

void DataTypeNullable::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type.get()->serializeBinary(*(col->getNestedColumn().get()), row_num, ostr);
}

void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type.get()->deserializeBinary(*(col->getNestedColumn().get()), istr);
}

void DataTypeNullable::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	const auto & null_map = content.getData();

	if (isNullValue(&null_map, row_num))
		writeCString(NullSymbol::Plain::name, ostr);
	else
		nested_data_type.get()->serializeTextEscaped(*(col->getNestedColumn().get()), row_num, ostr);
}

void DataTypeNullable::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	auto & null_map = content.getData();

	if (NullSymbol::Deserializer<NullSymbol::Escaped>::execute(column, istr, &null_map))
		col->insertDefault();
	else
		nested_data_type.get()->deserializeTextEscaped(*(col->getNestedColumn().get()), istr);
}

void DataTypeNullable::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	const auto & null_map = content.getData();

	if (isNullValue(&null_map, row_num))
		writeCString(NullSymbol::Quoted::name, ostr);
	else
		nested_data_type.get()->serializeTextQuoted(*(col->getNestedColumn().get()), row_num, ostr);
}

void DataTypeNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	auto & null_map = content.getData();

	if (NullSymbol::Deserializer<NullSymbol::Quoted>::execute(column, istr, &null_map))
		col->insertDefault();
	else
		nested_data_type.get()->deserializeTextQuoted(*(col->getNestedColumn().get()), istr);
}

void DataTypeNullable::serializeTextCSVImpl(const IColumn & column, size_t row_num,
	WriteBuffer & ostr, const NullValuesByteMap * null_map) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.get()->serializeTextCSV(*(col->getNestedColumn().get()), row_num, ostr, &content.getData());
}

void DataTypeNullable::deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr,
	const char delimiter, NullValuesByteMap * null_map) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.get()->deserializeTextCSV(*(col->getNestedColumn().get()), istr, delimiter, &content.getData());
}

void DataTypeNullable::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	const auto & null_map = content.getData();

	if (isNullValue(&null_map, row_num))
		writeCString(NullSymbol::Plain::name, ostr);
	else
		nested_data_type.get()->serializeText(*(col->getNestedColumn().get()), row_num, ostr);
}

void DataTypeNullable::serializeTextJSONImpl(const IColumn & column, size_t row_num,
	WriteBuffer & ostr, const NullValuesByteMap * null_map) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.get()->serializeTextJSON(*(col->getNestedColumn().get()), row_num, ostr, &content.getData());
}

void DataTypeNullable::deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr,
	NullValuesByteMap * null_map) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.get()->deserializeTextJSON(*(col->getNestedColumn().get()), istr, &content.getData());
}

void DataTypeNullable::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	auto & null_map = content.getData();

	if (isNullValue(&null_map, row_num))
		writeCString(NullSymbol::XML::name, ostr);
	else
		nested_data_type.get()->serializeTextXML(*(col->getNestedColumn().get()), row_num, ostr);
}

ColumnPtr DataTypeNullable::createColumn() const
{
	return std::make_shared<ColumnNullable>(nested_data_type.get()->createColumn());
}

ColumnPtr DataTypeNullable::createConstColumn(size_t size, const Field & field) const
{
	return std::make_shared<ColumnNullable>(nested_data_type.get()->createConstColumn(size, field));
}

Field DataTypeNullable::getDefault() const
{
	return nested_data_type.get()->getDefault();
}

size_t DataTypeNullable::getSizeOfField() const
{
	return nested_data_type.get()->getSizeOfField();
}

DataTypePtr & DataTypeNullable::getNestedType()
{
	return nested_data_type;
}

const DataTypePtr & DataTypeNullable::getNestedType() const
{
	return nested_data_type;
}

}
