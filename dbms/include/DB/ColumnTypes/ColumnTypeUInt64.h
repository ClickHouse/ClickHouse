#ifndef DBMS_COLUMN_TYPE_UINT64_H
#define DBMS_COLUMN_TYPE_UINT64_H

#include <DB/ColumnTypes/IColumnType.h>


namespace DB
{

/** Аналог BIGINT UNSIGNED, сериализуется в набор байт фиксированной длины */
class ColumnTypeUInt64 : public IColumnType
{
public:
	std::string getName() const { return "UInt64"; }
	
	void serializeBinary(const Field & field, std::ostream & ostr) const
	{
		Poco::BinaryWriter w(ostr);
		w << boost::get<UInt64>(field);
	}

	void deserializeBinary(Field & field, std::istream & istr) const
	{
		Poco::BinaryReader r(istr);
		UInt64 x;
		r >> x;
		field = x;
	}

	void serializeBinary(const Column & column, std::ostream & ostr) const
	{
		Poco::BinaryWriter w(ostr);
		const UInt64Column & column = boost::get<UInt64Column>(column);
		for (size_t i = 0, size = column.size(); i < size; ++i)
			w << column[i];
	}

	void deserializeBinary(Column & column, std::istream & istr) const
	{
		Poco::BinaryReader r(istr);
		UInt64Column & column = boost::get<UInt64Column>(column);
		for (size_t i = 0, size = column.size(); i < size; ++i)
			r >> column[i];
	}

	void serializeText(const Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<UInt64>(field);
	}

	void deserializeText(Field & field, std::istream & istr) const
	{
		UInt64 x;
		istr >> x;
		field = x;
	}
};

}

#endif
