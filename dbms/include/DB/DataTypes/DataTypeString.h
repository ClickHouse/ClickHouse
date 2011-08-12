#ifndef DBMS_DATA_TYPES_DATATYPE_STRING_H
#define DBMS_DATA_TYPES_DATATYPE_STRING_H

#include <ostream>

#include <Poco/SharedPtr.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;


class DataTypeString : public IDataType
{
public:
	std::string getName() const
	{
		return "String";
	}

	DataTypePtr clone() const
	{
		return new DataTypeString;
	}

	void serializeBinary(const Field & field, WriteBuffer & ostr) const;
	void deserializeBinary(Field & field, ReadBuffer & istr) const;
	void serializeBinary(const IColumn & column, WriteBuffer & ostr) const;
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

#endif
