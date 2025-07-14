#include <Common/Macros.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemMacros.h>


namespace DB
{

ColumnsDescription StorageSystemMacros::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"macro", std::make_shared<DataTypeString>(), "The macro name."},
        {"substitution", std::make_shared<DataTypeString>(), "The substitution string."},
    };
}

void StorageSystemMacros::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto macros = context->getMacros();

    for (const auto & macro : macros->getMacroMap())
    {
        res_columns[0]->insert(macro.first);
        res_columns[1]->insert(macro.second);
    }
}

}
