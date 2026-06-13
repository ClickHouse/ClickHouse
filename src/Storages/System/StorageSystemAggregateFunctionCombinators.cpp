#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemAggregateFunctionCombinators.h>

namespace DB
{

ColumnsDescription StorageSystemAggregateFunctionCombinators::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the combinator."},
        {"is_internal", std::make_shared<DataTypeUInt8>(), "Whether this combinator is for internal usage only."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description of what the combinator does."},
        {"syntax", std::make_shared<DataTypeString>(), "How the combinator is applied to an aggregate function name."},
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"introduced_in", std::make_shared<DataTypeString>(), "The ClickHouse version in which the combinator was first introduced, in the form major.minor."},
        {"related", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related combinators."},
    };
}

void StorageSystemAggregateFunctionCombinators::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & combinators = AggregateFunctionCombinatorFactory::instance().getAllAggregateFunctionCombinators();
    for (const auto & pair : combinators)
    {
        const auto & documentation = pair.documentation;

        size_t i = 0;
        res_columns[i++]->insert(pair.name);
        res_columns[i++]->insert(pair.combinator_ptr->isForInternalUsageOnly());
        res_columns[i++]->insert(documentation.description);
        res_columns[i++]->insert(documentation.syntaxAsString());
        res_columns[i++]->insert(documentation.examplesAsString());
        res_columns[i++]->insert(documentation.introducedInAsString());

        Array related;
        for (const auto & related_name : documentation.related)
            related.push_back(related_name);
        res_columns[i++]->insert(related);
    }
}

}
