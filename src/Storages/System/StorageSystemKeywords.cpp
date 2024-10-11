#include <Common/Macros.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemKeywords.h>

#include <Parsers/CommonParsers.h>


namespace DB
{

ColumnsDescription StorageSystemKeywords::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"keyword", std::make_shared<DataTypeString>(), "The keyword used in ClickHouse parser."},
    };
}

void StorageSystemKeywords::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto macros = context->getMacros();

    for (const auto & keyword : getAllKeyWords())
    {
        res_columns[0]->insert(keyword);
    }
}

}
