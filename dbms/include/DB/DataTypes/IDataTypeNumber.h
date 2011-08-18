#ifndef DBMS_DATA_TYPES_IDATATYPE_NUMBER_H
#define DBMS_DATA_TYPES_IDATATYPE_NUMBER_H

#include <DB/DataTypes/IDataType.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{


/** Реализует часть интерфейса IDataType, общую для всяких чисел
  * - ввод и вывод в текстовом виде.
  */
template <typename FType>
class IDataTypeNumber : public IDataType
{
public:
	typedef FType FieldType;

	bool isNumeric() const { return true; }
		
	void serializeText(const Field & field, WriteBuffer & ostr) const
	{
		writeText(boost::get<typename NearestFieldType<FieldType>::Type>(field), ostr);
	}
	
	void deserializeText(Field & field, ReadBuffer & istr) const
	{
		typename NearestFieldType<FieldType>::Type x;
		readText(x, istr);
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
		serializeText(field, ostr);
	}
	
	void deserializeTextQuoted(Field & field, ReadBuffer & istr, bool compatible = false) const
	{
		deserializeText(field, istr);
	}
};

}

#endif
