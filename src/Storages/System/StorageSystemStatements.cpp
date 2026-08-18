#include <Storages/System/StorageSystemStatements.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/StatementFactory.h>

namespace DB
{

ColumnsDescription StorageSystemStatements::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the SQL statement."},
        {"syntax", std::make_shared<DataTypeString>(), "The syntax of the statement."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description of what the statement does."},
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"parent_statement", std::make_shared<DataTypeString>(), "The name of the enclosing statement, e.g. SELECT for the WHERE clause. Empty for a top-level statement."},
        {"related_information", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related statements."},
    };
}

void StorageSystemStatements::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = StatementFactory::instance();

    for (const auto & name : factory.getAllRegisteredNames())
    {
        const auto documentation = factory.getDocumentation(name);

        size_t i = 0;
        res_columns[i++]->insert(name);
        res_columns[i++]->insert(documentation.syntaxAsString());
        res_columns[i++]->insert(documentation.description);
        res_columns[i++]->insert(documentation.examplesAsString());
        res_columns[i++]->insert(documentation.parent);

        Array related;
        for (const auto & related_name : documentation.related)
            related.push_back(related_name);
        res_columns[i++]->insert(related);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemStatements) }
