#ifndef DBMS_COLUMN_TYPES_NUMBER_FIXED_H
#define DBMS_COLUMN_TYPES_NUMBER_FIXED_H

#include <DB/Core/Column.h>
#include <DB/ColumnTypes/IColumnTypeNumberFixed.h>


namespace DB
{

/** Типы столбцов для чисел фиксированной ширины. */

#define DEFINE_COLUMN_TYPE_NUMBER_FIXED(TYPE) 										\
	class ColumnType ## TYPE : public IColumnTypeNumberFixed<TYPE, TYPE ## Column>	\
	{																				\
	public:																			\
		std::string getName() const { return #TYPE; }								\
	};

DEFINE_COLUMN_TYPE_NUMBER_FIXED(UInt8);
DEFINE_COLUMN_TYPE_NUMBER_FIXED(UInt16);
DEFINE_COLUMN_TYPE_NUMBER_FIXED(UInt32);
DEFINE_COLUMN_TYPE_NUMBER_FIXED(UInt64);

DEFINE_COLUMN_TYPE_NUMBER_FIXED(Int8);
DEFINE_COLUMN_TYPE_NUMBER_FIXED(Int16);
DEFINE_COLUMN_TYPE_NUMBER_FIXED(Int32);
DEFINE_COLUMN_TYPE_NUMBER_FIXED(Int64);

DEFINE_COLUMN_TYPE_NUMBER_FIXED(Float32);
DEFINE_COLUMN_TYPE_NUMBER_FIXED(Float64);

}

#endif
