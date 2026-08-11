#include <Storages/System/StorageSystemDataSkippingIndexTypes.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

ColumnsDescription StorageSystemDataSkippingIndexTypes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the data skipping index type, as specified in the TYPE of an INDEX declaration."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description of what the data skipping index type does."},
        {"syntax", std::make_shared<DataTypeString>(), "How the index is declared in an INDEX clause of a CREATE TABLE query."},
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"introduced_in", std::make_shared<DataTypeString>(), "The ClickHouse version in which the index type was first introduced, in the form major.minor."},
        {"related", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related data skipping index types."},
    };
}

void StorageSystemDataSkippingIndexTypes::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = MergeTreeIndexFactory::instance();
    for (const auto & name : factory.getAllRegisteredNames())
    {
        const auto documentation = factory.getDocumentation(name);

        size_t i = 0;
        res_columns[i++]->insert(name);
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
