#include <Common/Macros.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemMacros.h>


namespace DB
{

NamesAndTypesList StorageSystemMacros::getNamesAndTypes()
{
    return {
        {"macro", std::make_shared<DataTypeString>()},
        {"substitution", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemMacros::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    auto macros = context.getMacros();

    for (const auto & macro : macros->getMacroMap())
    {
        res_columns[0]->insert(macro.first);
        res_columns[1]->insert(macro.second);
    }
}

}
