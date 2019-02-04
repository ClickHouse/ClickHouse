#include <Interpreters/FindIdentifierBestTableVisitor.h>
#include <Interpreters/IdentifierSemantic.h>


namespace DB
{

FindIdentifierBestTableData::FindIdentifierBestTableData(const std::vector<DatabaseAndTableWithAlias> & tables_)
    : tables(tables_)
{
}

void FindIdentifierBestTableData::visit(ASTIdentifier & identifier, ASTPtr &)
{
    const DatabaseAndTableWithAlias * best_table = nullptr;

    if (!identifier.compound())
    {
        if (!tables.empty())
            best_table = &tables[0];
    }
    else
    {
        size_t best_match = 0;
        for (const DatabaseAndTableWithAlias & table : tables)
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
