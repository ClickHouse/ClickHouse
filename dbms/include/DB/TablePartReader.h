#ifndef DBMS_TABLE_PART_READER_H
#define DBMS_TABLE_PART_READER_H

#include <DB/Row.h>


namespace DB
{

class ITablePartReader
{
public:
	/// прочитать следующую строку, вернуть false, если строк больше нет
	virtual bool fetch(Row & row) = 0;

	virtual ~ITablePartReader() {}
};

}

#endif
