#ifndef DBMS_CORE_COLUMN_VARIANT_H
#define DBMS_CORE_COLUMN_VARIANT_H

#include <DB/Core/Field.h>
#include <DB/Columns/ColumnVector.h>


namespace DB
{

/** Столбец значений произвольного типа. */

typedef ColumnVector<Field> ColumnVariant;

}

#endif
