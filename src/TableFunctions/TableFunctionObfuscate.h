#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <base/types.h>

namespace DB
{

/* obfuscate(query)
 * Obfuscates original data from a subquery.
 */
class TableFunctionObfuscate : public ITableFunction
{
public:
    static constexpr auto name = "obfuscate";
    std::string getName() const override { return name; }

    const ASTSelectWithUnionQuery & getSelectQuery() const;

    /// `obfuscate` must not be persisted through `CREATE TABLE ... AS obfuscate(...)`. A table created
    /// from a table function builds its nested storage lazily, from the *global* context captured at
    /// DDL time (`InterpreterCreateQuery` passes `use_global_context = true`, because there is no query
    /// context on server startup), while `obfuscate` interprets its query argument - so reading such a
    /// table fails with `THERE_IS_NO_QUERY` instead of returning rows. The stored definition would also
    /// not be equivalent to the transient one: the query-construction settings (`select` / `filter` /
    /// `order` / `sort` / `limit` / `offset` / `page`) of the inner query are materialized by wrapping
    /// it as a derived table, and `executeQuery` does that only for a directly executed query - it stops
    /// at a `CREATE` whose source is a table function - so e.g. `SETTINGS limit = 2` would be silently
    /// ignored. There is no stable persisted representation, so forbid it, as `eval` does. A stored
    /// `VIEW` over `obfuscate(...)` is unaffected: it is expanded into the reading query.
    bool canBeUsedToCreateTable() const override { return false; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageEngineName() const override { return "Obfuscate"; }

    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    ASTCreateQuery create;
};


}
