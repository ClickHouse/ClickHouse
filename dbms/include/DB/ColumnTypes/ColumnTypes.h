#ifndef DBMS_COLUMN_TYPES_COLUMNTYPES_H
#define DBMS_COLUMN_TYPES_COLUMNTYPES_H

#include <vector>

#include <Poco/SharedPtr.h>

#include <DB/ColumnTypes/IColumnType.h>


namespace DB
{

using Poco::SharedPtr;

typedef std::vector<SharedPtr<IColumnType> > ColumnTypes;

}

#endif
