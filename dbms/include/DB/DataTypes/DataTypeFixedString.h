#pragma once

#include <ostream>

#include <Poco/SharedPtr.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;


class DataTypeFixedString : public IDataType
{
private:
	size_t n;

public:
	DataTypeFixedString(size_t n_) : n(n_)
	{
		if (n == 0)
			throw Exception("FixedString size must be positive", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
	}

	std::string getName() const
	{
		return "FixedString(" + toString(n) + ")";
	}

	DataTypePtr clone() const
	{
		return new DataTypeFixedString(n);
	}

	size_t getN() const
	{
		return n;
	}

	void serializeBinary(const Field & field, WriteBuffer & ostr) const;
	void deserializeBinary(Field & field, ReadBuffer & istr) const;
	void serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset = 0, size_t limit = 0) const;
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const;

	void serializeText(const Field & field, WriteBuffer & ostr) const;
	void deserializeText(Field & field, ReadBuffer & istr) const;

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const;
	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const;

	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const;
	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const;

	void serializeTextJSON(const Field & field, WriteBuffer & ostr) const;

	ColumnPtr createColumn() const;
	ColumnPtr createConstColumn(size_t size, const Field & field) const;

	Field getDefault() const
	{
		return String("");
	}
};

}
