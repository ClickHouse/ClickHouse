#pragma once

#include <ostream>

#include <Poco/SharedPtr.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;


class DataTypeFixedString final : public IDataType
{
private:
	size_t n;

public:
	DataTypeFixedString(size_t n_) : n(n_)
	{
		if (n == 0)
			throw Exception("FixedString size must be positive", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
	}

	std::string getName() const override;

	DataTypePtr clone() const override
	{
		return new DataTypeFixedString(n);
	}

	size_t getN() const
	{
		return n;
	}

	void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
	void deserializeBinary(Field & field, ReadBuffer & istr) const override;
	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const override;
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

	void serializeText(const Field & field, WriteBuffer & ostr) const override;
	void deserializeText(Field & field, ReadBuffer & istr) const override;

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const override;
	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const override;

	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const override;
	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const override;

	void serializeTextJSON(const Field & field, WriteBuffer & ostr) const override;

	ColumnPtr createColumn() const override;
	ColumnPtr createConstColumn(size_t size, const Field & field) const override;

	Field getDefault() const override
	{
		return String();
	}
};

}
