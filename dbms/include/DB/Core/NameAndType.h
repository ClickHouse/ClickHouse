#ifndef DBMS_CORE_NAME_AND_TYPE_H
#define DBMS_CORE_NAME_AND_TYPE_H

#include <Poco/SharedPtr.h>

#include <DB/Core/Column.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

/** Имя столбца и тип столбца.
  */

struct NameAndType
{
	DataTypePtr type;
	String name;
};

}

#endif
