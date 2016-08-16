#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/DataTypes/NullSymbol.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/Common/typeid_cast.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>

namespace DB
{

namespace
{

template <typename Null>
struct Deserializer
{
	static bool execute(IColumn & column, ReadBuffer & istr, NullValuesByteMap * null_map)
	{
		if (null_map != nullptr)
		{
			if (!istr.eof())
			{
				if (*istr.position() == Null::prefix)
				{
					++istr.position();
					if (Null::length > 1)
						assertString(Null::suffix, istr);
					null_map->push_back(1);
					return true;
				}
				else
				{
					null_map->push_back(0);
					return false;
				}
			}
		}

		return false;
	}
};

inline bool isNullValue(const NullValuesByteMap * null_map, size_t row_num)
{
	return (null_map != nullptr) && ((*null_map)[row_num] == 1);
}

}

DataTypeNullable::DataTypeNullable(DataTypePtr nested_data_type_)
	: nested_data_type{nested_data_type_}
{
}

std::string DataTypeNullable::getName() const
{
	return "Nullable(" + nested_data_type->getName() + ")";
}

bool DataTypeNullable::isNullable() const
{
	return true;
}

bool DataTypeNullable::isNumeric() const
{
	return nested_data_type->isNumeric();
}

bool DataTypeNullable::behavesAsNumber() const
{
	return nested_data_type->behavesAsNumber();
}

DataTypePtr DataTypeNullable::clone() const
{
	return std::make_shared<DataTypeNullable>(nested_data_type->clone());
}

void DataTypeNullable::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type->serializeBinary(*col->getNestedColumn(), ostr, offset, limit);
}

void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type->deserializeBinary(*col->getNestedColumn(), istr, limit, avg_value_size_hint);
}

void DataTypeNullable::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	nested_data_type->serializeBinary(field, ostr);
}

void DataTypeNullable::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	nested_data_type->deserializeBinary(field, istr);
}

void DataTypeNullable::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type->serializeBinary(*col->getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	nested_data_type->deserializeBinary(*col->getNestedColumn(), istr);
}

void DataTypeNullable::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*col->getNullValuesByteMap());
	const auto & null_map = content.getData();

	if (isNullValue(&null_map, row_num))
		writeCString(NullSymbol::Escaped::name, ostr);
	else
		nested_data_type->serializeTextEscaped(*col->getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*col->getNullValuesByteMap());
	auto & null_map = content.getData();

	if (Deserializer<NullSymbol::Escaped>::execute(column, istr, &null_map))
	{
		ColumnPtr & nested_col = col->getNestedColumn();
		nested_col->insertDefault();
	}
	else
		nested_data_type->deserializeTextEscaped(*col->getNestedColumn(), istr);
}

void DataTypeNullable::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*col->getNullValuesByteMap());
	const auto & null_map = content.getData();

	if (isNullValue(&null_map, row_num))
		writeCString(NullSymbol::Quoted::name, ostr);
	else
		nested_data_type->serializeTextQuoted(*col->getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*col->getNullValuesByteMap());
	auto & null_map = content.getData();

	if (Deserializer<NullSymbol::Quoted>::execute(column, istr, &null_map))
	{
		ColumnPtr & nested_col = col->getNestedColumn();
		nested_col->insertDefault();
	}
	else
		nested_data_type->deserializeTextQuoted(*col->getNestedColumn(), istr);
}

void DataTypeNullable::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*col->getNullValuesByteMap());
	const auto & null_map = content.getData();

	if (isNullValue(&null_map, row_num))
		writeCString(NullSymbol::Quoted::name, ostr);
	else
		nested_data_type->serializeTextCSV(*col->getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*col->getNullValuesByteMap());
	auto & null_map = content.getData();

	if (Deserializer<NullSymbol::Quoted>::execute(column, istr, &null_map))
	{
		ColumnPtr & nested_col = col->getNestedColumn();
		nested_col->insertDefault();
	}
	else
		nested_data_type->deserializeTextCSV(*col->getNestedColumn(), istr, delimiter);
}

void DataTypeNullable::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*col->getNullValuesByteMap());
	const auto & null_map = content.getData();

	if (isNullValue(&null_map, row_num))
		writeCString(NullSymbol::Plain::name, ostr);
	else
		nested_data_type->serializeText(*col->getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*col->getNullValuesByteMap());
	const auto & null_map = content.getData();

	if (isNullValue(&null_map, row_num))
		writeCString(NullSymbol::JSON::name, ostr);
	else
		nested_data_type->serializeTextJSON(*col->getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable * col = typeid_cast<ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnUInt8 & content = static_cast<ColumnUInt8 &>(*col->getNullValuesByteMap());
	auto & null_map = content.getData();

	if (Deserializer<NullSymbol::JSON>::execute(column, istr, &null_map))
	{
		ColumnPtr & nested_col = col->getNestedColumn();
		nested_col->insertDefault();
	}
	else
		nested_data_type->deserializeTextJSON(*col->getNestedColumn(), istr);
}

void DataTypeNullable::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable * col = typeid_cast<const ColumnNullable *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	const ColumnUInt8 & content = static_cast<const ColumnUInt8 &>(*col->getNullValuesByteMap());
	auto & null_map = content.getData();

	if (isNullValue(&null_map, row_num))
		writeCString(NullSymbol::XML::name, ostr);
	else
		nested_data_type->serializeTextXML(*col->getNestedColumn(), row_num, ostr);
}

ColumnPtr DataTypeNullable::createColumn() const
{
	ColumnPtr new_col = nested_data_type->createColumn();
	return std::make_shared<ColumnNullable>(new_col, std::make_shared<ColumnUInt8>());
}

ColumnPtr DataTypeNullable::createConstColumn(size_t size, const Field & field) const
{
	ColumnPtr new_col = nested_data_type->createConstColumn(size, field);
	return std::make_shared<ColumnNullable>(new_col, std::make_shared<ColumnUInt8>(size));
}

Field DataTypeNullable::getDefault() const
{
	return nested_data_type->getDefault();
}

size_t DataTypeNullable::getSizeOfField() const
{
	return nested_data_type->getSizeOfField();
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
