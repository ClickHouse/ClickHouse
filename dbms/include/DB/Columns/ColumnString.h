#ifndef DBMS_CORE_COLUMN_STRING_H
#define DBMS_CORE_COLUMN_STRING_H

#include <DB/Core/Types.h>
#include <DB/Columns/ColumnVector.h>


namespace DB
{

/** Столбец строк. */

typedef ColumnVector<String> ColumnString;

}

#endif
