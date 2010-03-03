#ifndef DBMS_COLUMN_TYPES_ICOLUMNTYPE_NUMBER_H
#define DBMS_COLUMN_TYPES_ICOLUMNTYPE_NUMBER_H

#include <DB/ColumnTypes/IColumnType.h>


namespace DB
{


/** Реализует часть интерфейса IColumnType, общую для всяких чисел
  * - ввод и вывод в текстовом виде.
  */
template <typename FieldType>
class IColumnTypeNumber : public IColumnType
{
public:
	void serializeText(const Field & field, std::ostream & ostr) const
	{
		ostr << boost::get<FieldType>(field);
	}
	
	void deserializeText(Field & field, std::istream & istr) const
	{
		FieldType x;
		istr >> x;
		field = x;
	}

	void serializeTextEscaped(const Field & field, std::ostream & ostr) const
	{
		serializeText(field, ostr);
	}
	
	void deserializeTextEscaped(Field & field, std::istream & istr) const
	{
		deserializeText(field, istr);
	}
	
	void serializeTextQuoted(const Field & field, std::ostream & ostr, bool compatible = false) const
	{
		serializeText(field, ostr);
	}
	
	void deserializeTextQuoted(Field & field, std::istream & istr, bool compatible = false) const
	{
		deserializeText(field, istr);
	}
};

}

#endif
