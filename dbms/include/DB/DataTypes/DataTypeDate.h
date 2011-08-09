#pragma once

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/IDataTypeNumberFixed.h>


namespace DB
{

class DataTypeDate : public IDataTypeNumberFixed<UInt16, ColumnUInt16>
{
public:
	DataTypeDate() {}

	std::string getName() const { return "Date"; }
	DataTypePtr clone() const { return new DataTypeDate; }

	void serializeText(const Field & field, WriteBuffer & ostr) const
	{
		writeDateText(boost::get<UInt64>(field), ostr);
	}
	
	void deserializeText(Field & field, ReadBuffer & istr) const
	{
		Yandex::DayNum_t x;
		readDateText(x, istr);
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
