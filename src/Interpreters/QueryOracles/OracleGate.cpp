#include <Interpreters/QueryOracles/OracleGate.h>
#include <Interpreters/QueryOracles/OracleExec.h>

#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>

namespace DB
{

StoragePtr resolveSingleTableStorage(const ASTSelectQuery & select, const ContextPtr & context)
{
    const auto tables = select.tables();
    if (!tables || tables->children.size() != 1)
        return nullptr;

    const auto * elem = tables->children[0]->as<ASTTablesInSelectQueryElement>();
    if (!elem || elem->table_join || elem->array_join || !elem->table_expression)
        return nullptr;

    const auto * te = elem->table_expression->as<ASTTableExpression>();
    if (!te || te->subquery || te->table_function || !te->database_and_table_name)
        return nullptr;

    const auto * tid = te->database_and_table_name->as<ASTTableIdentifier>();
    if (!tid)
        return nullptr;

    String database = tid->getDatabaseName();
    if (database.empty())
        database = context->getCurrentDatabase();

    try
    {
        return DatabaseCatalog::instance().tryGetTable({database, tid->shortName()}, context);
    }
    catch (...)
    {
        return nullptr;
    }
}

ResolveMatch referencesTableMatching(
    const ASTPtr & ast, const ContextPtr & context,
    const std::function<bool(const StoragePtr &)> & predicate)
{
    if (!ast)
        return ResolveMatch::No;

    bool unresolvable = false;

    if (const auto * tid = ast->as<ASTTableIdentifier>())
    {
        String database = tid->getDatabaseName();
        if (database.empty())
            database = context->getCurrentDatabase();

        StoragePtr storage;
        try
        {
            storage = DatabaseCatalog::instance().tryGetTable({database, tid->shortName()}, context);
        }
        catch (...)
        {
            storage = nullptr;
        }

        if (storage)
        {
            if (predicate(storage))
                return ResolveMatch::Yes;
        }
        else
        {
            unresolvable = true;
        }
    }

    for (const auto & child : ast->children)
    {
        switch (referencesTableMatching(child, context, predicate))
        {
            case ResolveMatch::Yes:
                return ResolveMatch::Yes;
            case ResolveMatch::Unresolvable:
                unresolvable = true;
                break;
            case ResolveMatch::No:
                break;
        }
    }

    return unresolvable ? ResolveMatch::Unresolvable : ResolveMatch::No;
}

bool hasTotalOrderKey(
    const std::string & inner_sql,
    const std::vector<std::string> & key_exprs,
    const std::vector<std::string> & projection_exprs,
    const ContextMutablePtr & context)
{
    if (key_exprs.empty() || projection_exprs.empty())
        return false;

    auto join = [](const std::vector<std::string> & parts)
    {
        std::string out;
        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (i)
                out += ", ";
            out += parts[i];
        }
        return out;
    };

    /// One group per key => every group must render exactly one distinct projected tuple.
    const std::string sql =
        "SELECT max(d) FROM (SELECT uniqExact(tuple(" + join(projection_exprs)
        + ")) AS d FROM (" + inner_sql + ") GROUP BY " + join(key_exprs) + ")";

    std::optional<Field> value;
    try
    {
        value = OracleExec::executeScalar(sql, context);
    }
    catch (...)
    {
        return false;
    }

    if (!value || value->isNull())
        return false;

    try
    {
        return value->safeGet<UInt64>() == 1;
    }
    catch (...)
    {
        return false;
    }
}

}
