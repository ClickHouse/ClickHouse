#ifndef DBMS_CORE_COLUMN_STRING_H
#define DBMS_CORE_COLUMN_STRING_H

#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnsNumber.h>


namespace DB
{

/** Cтолбeц значений типа "строка".
  * Отличается от массива UInt8 только получением элемента (в виде String, а не Array)
  */
class ColumnString : public ColumnArray
{
public:
	/** Создать пустой столбец строк, с типом значений */
	ColumnString()
		: ColumnArray(new ColumnUInt8())
	{
	}
	
	Field operator[](size_t n) const
	{
		size_t offset = n == 0 ? 0 : offsets[n - 1];
		size_t size = offsets[n] - offset;
		const char * s = reinterpret_cast<const char *>(&dynamic_cast<const ColumnUInt8 &>(*data).getData()[offset]);
		return String(s, size);
	}
};


}

#endif
