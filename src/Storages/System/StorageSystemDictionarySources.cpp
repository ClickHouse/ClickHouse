#include <Storages/System/StorageSystemDictionarySources.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Dictionaries/DictionarySourceFactory.h>

namespace DB
{

ColumnsDescription StorageSystemDictionarySources::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the dictionary source, as specified in the SOURCE clause."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description of what the dictionary source does."},
        {"syntax", std::make_shared<DataTypeString>(), "The structure of the SOURCE clause used to specify the source. Note that some sources are subject to access control when a dictionary is created from a DDL query (as opposed to a server configuration file); see the `description` of the individual source for details."},
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"introduced_in", std::make_shared<DataTypeString>(), "The ClickHouse version in which the source was first introduced, in the form major.minor."},
        {"related", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related dictionary sources."},
    };
}

void StorageSystemDictionarySources::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = DictionarySourceFactory::instance();
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
