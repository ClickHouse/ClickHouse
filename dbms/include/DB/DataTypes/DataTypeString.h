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

	void serializeBinary(const Field & field, std::ostream & ostr) const;
	void deserializeBinary(Field & field, std::istream & istr) const;
	void serializeBinary(const IColumn & column, std::ostream & ostr) const;
	void deserializeBinary(IColumn & column, std::istream & istr, size_t limit) const;

	void serializeText(const Field & field, std::ostream & ostr) const;
	void deserializeText(Field & field, std::istream & istr) const;

	void serializeTextEscaped(const Field & field, std::ostream & ostr) const;
	void deserializeTextEscaped(Field & field, std::istream & istr) const;

	void serializeTextQuoted(const Field & field, std::ostream & ostr, bool compatible = false) const;
	void deserializeTextQuoted(Field & field, std::istream & istr, bool compatible = false) const;

	SharedPtr<IColumn> createColumn() const;
};

}

#endif
