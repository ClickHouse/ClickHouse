#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/Common/typeid_cast.h>

namespace DB
{

DataTypeNullable::DataTypeNullable(DataTypePtr nested_data_type_holder_)
	: nested_data_type_holder{nested_data_type_holder_},
	nested_data_type{*(nested_data_type_holder.get())}
{
}

std::string DataTypeNullable::getName() const
{
	return "Nullable(" + nested_data_type.getName() + ")";
}

bool DataTypeNullable::isNullable() const
{
	return true;
}

bool DataTypeNullable::isNumeric() const
{
	return nested_data_type.isNumeric();
}

bool DataTypeNullable::behavesAsNumber() const
{
	return nested_data_type.behavesAsNumber();
}

DataTypePtr DataTypeNullable::clone() const
{
	return std::make_shared<DataTypeNullable>(nested_data_type.clone());
}

void DataTypeNullable::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type.serializeBinary(*(col->getNestedColumn().get()), ostr, offset, limit);
}

void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type.deserializeBinary(*(col->getNestedColumn().get()), istr, limit, avg_value_size_hint);
}

void DataTypeNullable::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	nested_data_type.serializeBinary(field, ostr);
}

void DataTypeNullable::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	nested_data_type.deserializeBinary(field, istr);
}

void DataTypeNullable::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type.serializeBinary(*(col->getNestedColumn().get()), row_num, ostr);
}

void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type.deserializeBinary(*(col->getNestedColumn().get()), istr);
}

void DataTypeNullable::serializeTextEscapedImpl(const IColumn & column, size_t row_num,
	WriteBuffer & ostr, const NullValuesByteMap * null_map) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.serializeTextEscaped(*(col->getNestedColumn().get()), row_num, ostr, &content.getData());
}

void DataTypeNullable::deserializeTextEscapedImpl(IColumn & column, ReadBuffer & istr,
	NullValuesByteMap * null_map) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.deserializeTextEscaped(*(col->getNestedColumn().get()), istr, &content.getData());
}

void DataTypeNullable::serializeTextQuotedImpl(const IColumn & column, size_t row_num,
	WriteBuffer & ostr, const NullValuesByteMap * null_map) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.serializeTextQuoted(*(col->getNestedColumn().get()), row_num, ostr, &content.getData());
}

void DataTypeNullable::deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr,
	NullValuesByteMap * null_map) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.deserializeTextQuoted(*(col->getNestedColumn().get()), istr, &content.getData());
}

void DataTypeNullable::serializeTextCSVImpl(const IColumn & column, size_t row_num,
	WriteBuffer & ostr, const NullValuesByteMap * null_map) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.serializeTextCSV(*(col->getNestedColumn().get()), row_num, ostr, &content.getData());
}

void DataTypeNullable::deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr,
	const char delimiter, NullValuesByteMap * null_map) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.deserializeTextCSV(*(col->getNestedColumn().get()), istr, delimiter, &content.getData());
}

void DataTypeNullable::serializeTextImpl(const IColumn & column, size_t row_num,
	WriteBuffer & ostr, const NullValuesByteMap * null_map) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.serializeText(*(col->getNestedColumn().get()), row_num, ostr, &content.getData());
}

void DataTypeNullable::serializeTextJSONImpl(const IColumn & column, size_t row_num,
	WriteBuffer & ostr, const NullValuesByteMap * null_map) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.serializeTextJSON(*(col->getNestedColumn().get()), row_num, ostr, &content.getData());
}

void DataTypeNullable::deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr,
	NullValuesByteMap * null_map) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.deserializeTextJSON(*(col->getNestedColumn().get()), istr, &content.getData());
}

void DataTypeNullable::serializeTextXMLImpl(const IColumn & column, size_t row_num,
	WriteBuffer & ostr, const NullValuesByteMap * null_map) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*(col->getNullValuesByteMap().get()));
	nested_data_type.serializeTextXML(*(col->getNestedColumn().get()), row_num, ostr, &content.getData());
}

ColumnPtr DataTypeNullable::createColumn() const
{
	return std::make_shared<ColumnNullable>(nested_data_type.createColumn());
}

ColumnPtr DataTypeNullable::createConstColumn(size_t size, const Field & field) const
{
	return std::make_shared<ColumnNullable>(nested_data_type.createConstColumn(size, field));
}

Field DataTypeNullable::getDefault() const
{
	return nested_data_type.getDefault();
}

size_t DataTypeNullable::getSizeOfField() const
{
	return nested_data_type.getSizeOfField();
}

DataTypePtr & DataTypeNullable::getNestedType()
{
	return nested_data_type_holder;
}

const DataTypePtr & DataTypeNullable::getNestedType() const
{
	return nested_data_type_holder;
}

}
