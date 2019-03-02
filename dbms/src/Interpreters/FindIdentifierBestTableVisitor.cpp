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
        for (const auto & [table, names] : tables)
        {
            if (std::find(names.begin(), names.end(), identifier.name) != names.end())
            {
                // TODO: make sure no collision ever happens
                if (!best_table)
                    best_table = &table;
            }
        }
    }
    else
    {
        // FIXME: make a better matcher using `names`?
        size_t best_match = 0;
        for (const auto & [table, names] : tables)
        {
            if (size_t match = IdentifierSemantic::canReferColumnToTable(identifier, table))
                if (match > best_match)
                {
                    best_match = match;
                    best_table = &table;
                }
        }
    }

    identifier_table.emplace_back(&identifier, best_table);
}

}
