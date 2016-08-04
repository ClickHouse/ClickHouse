#include <DB/DataTypes/DataTypeNull.h>
#include <DB/DataTypes/NullSymbol.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

std::string DataTypeNull::getName() const
{
	return "Null";
}

bool DataTypeNull::isNull() const
{
	return true;
}

bool DataTypeNull::isNumeric() const
{
	return true;
}

bool DataTypeNull::behavesAsNumber() const
{
	return true;
}

DataTypePtr DataTypeNull::clone() const
{
	return std::make_shared<DataTypeNull>();
}

void DataTypeNull::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	size_t size = column.size();

	if ((limit == 0) || (offset + limit) > size)
		limit = size - offset;

	UInt8 x = 0 ;
	for (size_t i = 0; i < limit; ++i)
		ostr.write(reinterpret_cast<const char *>(&x), sizeof(UInt8) * 1);
}

void DataTypeNull::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnNull * col = typeid_cast<ColumnNull *>(&column);
	if (col == nullptr)
		throw Exception{"Discrepancy between data type and column type", ErrorCodes::LOGICAL_ERROR};

	ColumnNull & col_ref = *col;

	UInt8 x;
	for (size_t i = 0; i < limit; ++i)
	{
		istr.readBig(reinterpret_cast<char *>(&x), sizeof(UInt8) * 1);
		col_ref.insertDefault();
	}
}

void DataTypeNull::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	UInt8 x = 0;
	writeBinary(x, ostr);
}

void DataTypeNull::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	UInt8 x;
	readBinary(x, istr);
	field = Field{};
}

void DataTypeNull::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	UInt8 x = 0;
	writeBinary(x, ostr);
}

void DataTypeNull::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
	UInt8 x;
	readBinary(x, istr);
	column.insertDefault();
}

ColumnPtr DataTypeNull::createColumn() const
{
	return std::make_shared<ColumnNull>(0, Null());
}

ColumnPtr DataTypeNull::createConstColumn(size_t size, const Field & field) const
{
	return std::make_shared<ColumnNull>(size, Null());
}

Field DataTypeNull::getDefault() const
{
	return Field{};
}

size_t DataTypeNull::getSizeOfField() const
{
	/// NULL has the size of the smallest non-null type.
	return sizeof(UInt8);
}

void DataTypeNull::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeCString(NullSymbol::Escaped::name, ostr);
}

void DataTypeNull::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
	assertString(NullSymbol::Escaped::name, istr);
}

void DataTypeNull::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeCString(NullSymbol::Quoted::name, ostr);
}

void DataTypeNull::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
	assertString(NullSymbol::Quoted::name, istr);
}

void DataTypeNull::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeCString(NullSymbol::CSV::name, ostr);
}

void DataTypeNull::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
	assertString(NullSymbol::CSV::name, istr);
}

void DataTypeNull::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeCString(NullSymbol::Plain::name, ostr);
}

void DataTypeNull::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeCString(NullSymbol::JSON::name, ostr);
}

void DataTypeNull::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
	assertString(NullSymbol::JSON::name, istr);
}

}
