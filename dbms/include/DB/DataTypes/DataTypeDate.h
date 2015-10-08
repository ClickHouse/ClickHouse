#pragma once

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/IDataTypeNumberFixed.h>


namespace DB
{

class DataTypeDate final : public IDataTypeNumberFixed<UInt16, ColumnUInt16>
{
public:
	DataTypeDate() {}

	bool behavesAsNumber() const override { return false; }

	std::string getName() const override { return "Date"; }
	DataTypePtr clone() const override { return new DataTypeDate; }

	void serializeText(const Field & field, WriteBuffer & ostr) const override
	{
		writeDateText(DayNum_t(get<UInt64>(field)), ostr);
	}

	void deserializeText(Field & field, ReadBuffer & istr) const override
	{
		DayNum_t x;
		readDateText(x, istr);
		field = static_cast<UInt64>(x);
	}

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const override
	{
		serializeText(field, ostr);
	}

	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const override
	{
		deserializeText(field, istr);
	}

	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const override
	{
		writeChar('\'', ostr);
		serializeText(field, ostr);
		writeChar('\'', ostr);
	}

	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const override
	{
		assertString("'", istr);
		deserializeText(field, istr);
		assertString("'", istr);
	}

	void serializeTextJSON(const Field & field, WriteBuffer & ostr) const override
	{
		writeChar('"', ostr);
		serializeText(field, ostr);
		writeChar('"', ostr);
	}
};

}
