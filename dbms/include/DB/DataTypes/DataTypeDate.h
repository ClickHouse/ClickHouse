#ifndef DBMS_DATA_TYPES_NUMBER_FIXED_H
#define DBMS_DATA_TYPES_NUMBER_FIXED_H

#include <Poco/DateTimeParser.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/IDataTypeNumberFixed.h>


namespace DB
{

class DataTypeDate : public IDataTypeNumberFixed<UInt16, ColumnUInt64>
{
private:
	DateLUTSingleton & date_lut;

public:
	DataTypeDate() : date_lut(DateLUTSingleton::instance()) {}

	std::string getName() const { return "Date"; }
	SharedPtr<IDataType> clone() const { return new DataTypeDate; }

	void serializeText(const Field & field, WriteBuffer & ostr) const
	{
		DateLUT::Values & values = date_lut.getValues(boost::get<UInt16>(field));
		writeIntText(values.year, ostr);
		writeChar('-', ostr);
		writeIntText(values.month, ostr);
		writeChar('-', ostr);
		writeIntText(values.day_of_month, ostr);
	}
	
	void deserializeText(Field & field, ReadBuffer & istr) const
	{
		std::string s;
		readString(s, istr);

		// TODO: тормоза
		int time_zone_diff = 0;
		field = date_lut.toDayNum(Poco::DateTimeParser::parse(
			s, time_zone_diff).timestamp().epochTime());
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

#endif
