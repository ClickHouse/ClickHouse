#pragma once

#include <DB/DataTypes/IDataType.h>


namespace DB
{

/** Базовые класс для типов данных, которые не поддерживают сериализацию и десериализацию,
  *  а возникают лишь в качестве промежуточного результата вычислений.
  *
  * То есть, этот класс используется всего лишь чтобы отличить соответствующий тип данных от других.
  */
class IDataTypeDummy : public IDataType
{
public:
	void serializeBinary(const Field & field, WriteBuffer & ostr) const override
	{
		throwNoSerialization();
	}

	void deserializeBinary(Field & field, ReadBuffer & istr) const override
	{
		throwNoSerialization();
	}

	void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		throwNoSerialization();
	}

	void deserializeBinary(IColumn & column, ReadBuffer & istr) const override
	{
		throwNoSerialization();
	}

	void serializeBinary(const IColumn & column, WriteBuffer & ostr,
		size_t offset = 0, size_t limit = 0) const override
	{
		throwNoSerialization();
	}

	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override
	{
		throwNoSerialization();
	}

	ColumnPtr createColumn() const override
	{
		throw Exception("Method createColumn() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	ColumnPtr createConstColumn(size_t size, const Field & field) const override
	{
		throw Exception("Method createConstColumn() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	Field getDefault() const override
	{
		throw Exception("Method getDefault() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

protected:
	void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const NullValuesByteMap * null_map) const override
	{
		throwNoSerialization();
	}

	void serializeTextEscapedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const NullValuesByteMap * null_map) const override
	{
		throwNoSerialization();
	}

	void deserializeTextEscapedImpl(IColumn & column, ReadBuffer & istr,
		NullValuesByteMap * null_map) const override
	{
		throwNoSerialization();
	}

	void serializeTextQuotedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const NullValuesByteMap * null_map) const override
	{
		throwNoSerialization();
	}

	void deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr,
		NullValuesByteMap * null_map) const override
	{
		throwNoSerialization();
	}

	void serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const NullValuesByteMap * null_map) const override
	{
		throwNoSerialization();
	}

	void deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr,
		NullValuesByteMap * null_map) const override
	{
		throwNoSerialization();
	}

	void serializeTextCSVImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const NullValuesByteMap * null_map) const override
	{
		throwNoSerialization();
	}

	void deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const char delimiter,
		NullValuesByteMap * null_map) const override
	{
		throwNoSerialization();
	}

private:
	void throwNoSerialization() const
	{
		throw Exception("Serialization is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}
};

}

