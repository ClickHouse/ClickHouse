#pragma once

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/IDataTypeNumberFixed.h>


namespace DB
{

class DataTypeDateTime : public IDataTypeNumberFixed<UInt32, ColumnUInt32>
{
public:
	DataTypeDateTime() {}

	std::string getName() const { return "DateTime"; }
	SharedPtr<IDataType> clone() const { return new DataTypeDateTime; }

	void serializeText(const Field & field, WriteBuffer & ostr) const
	{
		writeDateTimeText(boost::get<UInt64>(field), ostr);
	}
	
	void deserializeText(Field & field, ReadBuffer & istr) const
	{
		time_t x;
		readDateTimeText(x, istr);
		field = x;
	}

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
	{
		serializeText(field, ostr);
	}
	
	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const
	{
		deserializeText(field, istr);
	}
	
	void serializeTextQuoted(const Field & field, WriteBuffer & ostr, bool compatible = false) const
	{
		writeChar('\'', ostr);
		serializeText(field, ostr);
		writeChar('\'', ostr);
	}
	
	void deserializeTextQuoted(Field & field, ReadBuffer & istr, bool compatible = false) const
	{
		assertString("'", istr);
		deserializeText(field, istr);
		assertString("'", istr);
	}
};

}
