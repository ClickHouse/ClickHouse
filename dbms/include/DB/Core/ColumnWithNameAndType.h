#ifndef DBMS_CORE_COLUMN_WITH_NAME_AND_TYPE_H
#define DBMS_CORE_COLUMN_WITH_NAME_AND_TYPE_H

#include <Poco/SharedPtr.h>

#include <DB/Core/Column.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

/** Тип данных для представления столбца вместе с его типом и именем в оперативке.
  */

struct ColumnWithNameAndType
{
	SharedPtr<Column> column;
	SharedPtr<IDataType> type;
	String name;
};

}

#endif
