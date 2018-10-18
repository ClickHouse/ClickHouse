#pragma once

namespace DB
{

/// Finds in the query the usage of external tables (as table identifiers). Fills in external_tables.
class ExternalTablesVisitor
{
public:
    ExternalTablesVisitor(const Context & context_, Tables & tables)
    :   context(context_),
        external_tables(tables)
    {}

    void visit(ASTPtr & ast) const
    {
        /// Traverse from the bottom. Intentionally go into subqueries.
        for (auto & child : ast->children)
            visit(child);

        tryVisit<ASTIdentifier>(ast);
    }

private:
    const Context & context;
    Tables & external_tables;

    void visit(const ASTIdentifier * node, ASTPtr &) const
    {
        if (node->special())
            if (StoragePtr external_storage = context.tryGetExternalTable(node->name))
                external_tables[node->name] = external_storage;
    }

    template <typename T>
    bool tryVisit(ASTPtr & ast) const
    {
        if (const T * t = typeid_cast<const T *>(ast.get()))
        {
            visit(t, ast);
            return true;
        }
        return false;
    }
};

}
