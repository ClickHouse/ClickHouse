#include "config.h"

#if USE_NLP

#include <Storages/System/StorageSystemStemmers.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>

#include <libstemmer.h>


namespace DB
{

ColumnsDescription StorageSystemStemmers::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the stemmer language"}
    };
}

StorageSystemStemmers::StorageSystemStemmers(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id, getColumnsDescription())
{
}

void StorageSystemStemmers::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const char ** language = sb_stemmer_list(); *language != nullptr; ++language)
        res_columns[0]->insert(String(*language));
}

}

#endif
