#include "StorageSystemDDLWorkerQueue.h"

#include <algorithm>
#include <DataTypes/DataTypeString.h>


namespace DB
{
NamesAndTypesList StorageSystemDDLWorkerQueue::getNamesAndTypes()
{
    return {
        {"query", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemDDLWorkerQueue::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    // TODO: insert into table
}
}
