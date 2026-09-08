#include <Storages/System/StorageSystemDictionaryLayouts.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/DictionaryFactory.h>

namespace DB
{

ColumnsDescription StorageSystemDictionaryLayouts::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the dictionary layout, as specified in the LAYOUT clause."},
        {"is_complex", std::make_shared<DataTypeUInt8>(), "Whether the layout requires a complex key (a key consisting of several attributes or of a non-integer type)."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description of what the dictionary layout does."},
        {"syntax", std::make_shared<DataTypeString>(), "How the layout is specified in the LAYOUT clause of a CREATE DICTIONARY query."},
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"introduced_in", std::make_shared<DataTypeString>(), "The ClickHouse version in which the layout was first introduced, in the form major.minor."},
        {"related", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related dictionary layouts."},
    };
}

void StorageSystemDictionaryLayouts::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = DictionaryFactory::instance();
    for (const auto & name : factory.getAllRegisteredNames())
    {
        const auto documentation = factory.getDocumentation(name);

        size_t i = 0;
        res_columns[i++]->insert(name);
        res_columns[i++]->insert(factory.isComplex(name));
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
