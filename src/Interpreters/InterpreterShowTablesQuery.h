#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;


/** Return a list of tables or databases meets specified conditions.
  * Interprets a query through replacing it to SELECT query from system.tables or system.databases.
  */
class InterpreterShowTablesQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterShowTablesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    /// Ignore quota and limits here because execute() produces a SELECT query which checks quotas/limits by itself.
    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

    struct HierarchicalDatabaseSource
    {
        String condition;
        String name_expression;
        /// The databases the source reads from.
        Strings databases;
    };
    /// The sources of the tables listed for a hierarchical database name (see `DatabaseCatalog`), as conditions on
    /// a table with the columns `database_column` and `name_column` (`system.tables`, `INFORMATION_SCHEMA.TABLES`)
    /// and expressions of the listed name, relative to `database`. Empty for an ordinary database name, which is only itself.
    /// Also used by the MySQL and PostgreSQL protocol handlers to scope their catalog queries to the current database.
    static std::vector<HierarchicalDatabaseSource> getHierarchicalDatabaseSources(
        const String & database, const String & database_column = "database", const String & name_column = "name");

private:
    ASTPtr query_ptr;

    String getRewrittenQuery();
};


}
