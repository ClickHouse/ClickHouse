#include <Storages/System/StorageSystemTableFunctions.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <Documentation/TableDocumentationFactory.h>

namespace DB
{

NamesAndTypesList StorageSystemTableFunctions::getNamesAndTypes()
{
    return {
        {"name",           std::make_shared<DataTypeString>()},
        {"documentation" , std::make_shared<DataTypeString>()}
    };
}

void StorageSystemTableFunctions::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    const auto & functions_names = TableFunctionFactory::instance().getAllRegisteredNames();
    auto & doc_factory = TableDocumentationFactory::instance();
    for (const auto & function_name : functions_names)
    {
        res_columns[0]->insert(function_name);
        res_columns[0]->insert(doc_factory.tryGet(function_name));
    }
}

}
