#include <Interpreters/FindIdentifierBestTableVisitor.h>
#include <Interpreters/IdentifierSemantic.h>


namespace DB
{

FindIdentifierBestTableData::FindIdentifierBestTableData(const std::vector<TableWithColumnNames> & tables_)
    : tables(tables_)
{
}

void FindIdentifierBestTableData::visit(ASTIdentifier & identifier, ASTPtr &)
{
    const DatabaseAndTableWithAlias * best_table = nullptr;

    if (!identifier.compound())
    {
        for (const auto & table_names : tables)
        {
            if (std::find(table_names.second.begin(), table_names.second.end(), identifier.name) != table_names.second.end())
            {
                // TODO: make sure no collision ever happens
                if (!best_table)
                    best_table = &table_names.first;
            }
        }
    }
    else
    {
        // FIXME: make a better matcher using `names`?
        size_t best_match = 0;
        for (const auto & table_names : tables)
        {
            if (size_t match = IdentifierSemantic::canReferColumnToTable(identifier, table_names.first))
                if (match > best_match)
                {
                    best_match = match;
                    best_table = &table_names.first;
                }
        }
    }

    identifier_table.emplace_back(&identifier, best_table);
}

}
