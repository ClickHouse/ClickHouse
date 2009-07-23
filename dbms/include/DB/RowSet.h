#ifndef DBMS_ROW_SET_H
#define DBMS_ROW_SET_H

#include <map>
#include <vector>

#include <DB/Row.h>


namespace DB
{
	
/** Набор строк. */
typedef std::vector<Row> RowSet;

/** Набор строк, использующийся в качестве результата запроса с агрегатными функциями,
  * а также как массив для загрузки данных в таблицу.
  */
typedef std::map<Row, Row> AggregatedRowSet;

/** Битовая маска столбцов. */
typedef std::vector<bool> ColumnMask;

}

#endif
