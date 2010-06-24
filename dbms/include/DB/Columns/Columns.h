#ifndef DBMS_COLUMNS_COLUMNS_H
#define DBMS_COLUMNS_COLUMNS_H

#include <vector>
#include <Poco/SharedPtr.h>
#include <DB/Columns/IColumn.h>


namespace DB
{

using Poco::SharedPtr;

typedef std::vector<SharedPtr<IColumn> > Columns;

}

#endif
