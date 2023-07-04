#include <Common/Macros.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemKeywords.h>

#include <Parsers/CommonParsers.h>


namespace DB
{

NamesAndTypesList StorageSystemKeywords::getNamesAndTypes()
{
    return {
        {"keyword", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemKeywords::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto macros = context->getMacros();

    for (const auto & keyword : getAllKeyWords())
    {
        res_columns[0]->insert(keyword);
    }
}

}
