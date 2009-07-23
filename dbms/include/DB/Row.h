#ifndef DBMS_ROW_H
#define DBMS_ROW_H

#include <map>
#include <vector>

#include <DB/Field.h>


namespace DB
{
	
/** Строка или часть строки.
  * Используется для хранения строк в памяти
  * - при обработке запроса, для временных таблиц, для результата.
  */
typedef std::vector<Field> Row;

}

#endif
