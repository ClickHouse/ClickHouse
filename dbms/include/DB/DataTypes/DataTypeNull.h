#pragma once

#include <DB/DataTypes/IDataType.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

/// Data type which represents a single NULL value. It is the type
/// associated to a constant column that contains only NULL values,
/// namely ColumnNull, which arises when a NULL is specified as a
/// column in any query.
class DataTypeNull final : public IDataType
{
public:
	using FieldType = Null;

public:
	String getName() const override
	{
		return "Null";
	}

	bool isNull() const override
	{
		return true;
	}

	DataTypePtr clone() const override
	{
		return std::make_shared<DataTypeNull>();
	}

	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const override;
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

	ColumnPtr createColumn() const override
	{
		return std::make_shared<ColumnNull>(0, Null());
	}

	ColumnPtr createConstColumn(size_t size, const Field & field) const override
	{
		return std::make_shared<ColumnNull>(size, Null());
	}

	Field getDefault() const override
	{
		return Null();
	}

	size_t getSizeOfField() const override		/// TODO Check where it is needed.
	{
		/// NULL has the size of the smallest non-null type.
		return sizeof(UInt8);
	}

	void serializeBinary(const Field & field, WriteBuffer & ostr) const override
	{
		UInt8 x = 1;	/// Value is 1 to be consistent with NULLs serialization in DataTypeNullable.
		writeBinary(x, ostr);
	}

	void deserializeBinary(Field & field, ReadBuffer & istr) const override
	{
		UInt8 x;
		readBinary(x, istr);
		field = Null();
	}

	void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		UInt8 x = 1;
		writeBinary(x, ostr);
	}

	void deserializeBinary(IColumn & column, ReadBuffer & istr) const override
	{
		UInt8 x;
		readBinary(x, istr);
		column.insertDefault();
	}

	void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeCString("\\N", ostr);
	}

	void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override
	{
		assertString("\\N", istr);
	}

	void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeCString("NULL", ostr);
	}

	void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override
	{
		assertStringCaseInsensitive("NULL", istr);
	}

	void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeCString("\\N", ostr);
	}

	void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override
	{
		assertString("\\N", istr);
	}

	void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeCString("NULL", ostr);
	}

	void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		bool force_quoting_64bit_integers) const override
	{
		writeCString("null", ostr);
	}

	void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override
	{
		assertString("null", istr);
	}
};

}
