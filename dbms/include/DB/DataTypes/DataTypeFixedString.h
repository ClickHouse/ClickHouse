#pragma once

#include <ostream>

#include <Poco/SharedPtr.h>
#include <Poco/NumberFormatter.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;


class DataTypeFixedString : public IDataType
{
private:
	size_t n;
	
public:
	DataTypeFixedString(size_t n_) : n(n_) {}
	
	std::string getName() const
	{
		return "FixedString(" + Poco::NumberFormatter::format(n) + ")";
	}

	DataTypePtr clone() const
	{
		return new DataTypeFixedString(n);
	}

	void serializeBinary(const Field & field, WriteBuffer & ostr) const;
	void deserializeBinary(Field & field, ReadBuffer & istr) const;
	void serializeBinary(const IColumn & column, WriteBuffer & ostr, WriteCallback callback = WriteCallback()) const;
	void deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit) const;

	void serializeText(const Field & field, WriteBuffer & ostr) const;
	void deserializeText(Field & field, ReadBuffer & istr) const;

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const;
	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const;

	void serializeTextQuoted(const Field & field, WriteBuffer & ostr, bool compatible = false) const;
	void deserializeTextQuoted(Field & field, ReadBuffer & istr, bool compatible = false) const;

	ColumnPtr createColumn() const;
	ColumnPtr createConstColumn(size_t size, const Field & field) const;
};

}
