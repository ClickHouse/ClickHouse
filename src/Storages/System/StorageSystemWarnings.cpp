#include <memory>
#include <Storages/System/StorageSystemWarnings.h>
#include <Interpreters/Context.h>

namespace DB 
{

NamesAndTypesList StorageSystemWarnings::getNamesAndTypes()
{
    return {
        {"message", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemWarnings::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    for (auto& warning : context->getWarnings()) {
        res_columns[0]->insert("Warning: " + warning);
    }
}

}
