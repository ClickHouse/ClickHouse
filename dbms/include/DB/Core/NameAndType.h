#ifndef DBMS_CORE_NAME_AND_TYPE_H
#define DBMS_CORE_NAME_AND_TYPE_H

#include <Poco/SharedPtr.h>

#include <DB/Core/Column.h>
#include <DB/ColumnTypes/IColumnType.h>


namespace DB
{

using Poco::SharedPtr;

/** Имя столбца и тип столбца.
  */

struct NameAndType
{
	SharedPtr<IColumnType> type;
	String name;
};

}

#endif
