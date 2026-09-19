#include <Interpreters/MutationPredicateColumnsAccess.h>

#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <array>
#include <optional>
#include <unordered_set>

namespace DB
{

void addExpressionColumnsSelectAccess(
    AccessRightsElements & required_access,
    const IAST * expression,
    const String & database,
    const String & table,
    const StorageInMemoryMetadata & metadata)
{
    if (!expression)
        return;

    RequiredSourceColumnsVisitor::Data columns_context;
    auto expression_clone = expression->clone();
    RequiredSourceColumnsVisitor(columns_context).visit(expression_clone);

    Strings columns;
    const String db_table_prefix = database.empty() ? String{} : database + "." + table + ".";
    const String table_prefix = table + ".";
    for (const auto & name : columns_context.requiredColumns())
    {
        /// A real column (including a real dotted/quoted name like `t.id`) requires SELECT as-is.
        if (metadata.columns.has(name))
        {
            columns.emplace_back(name);
            continue;
        }

        /// A virtual column not shadowed by a real one needs no SELECT grant, as in a plain SELECT.
        if (metadata.isVirtualColumn(name))
            continue;

        /// Otherwise strip a `table.` / `db.table.` qualifier and resolve the bare name the same way.
        std::string_view bare = name;
        if (!db_table_prefix.empty() && bare.starts_with(db_table_prefix))
            bare.remove_prefix(db_table_prefix.size());
        else if (bare.starts_with(table_prefix))
            bare.remove_prefix(table_prefix.size());

        if (metadata.isVirtualColumn(String(bare)))
            continue;

        columns.emplace_back(bare);
    }

    if (!columns.empty())
        required_access.emplace_back(AccessType::SELECT, database, table, columns);
}

namespace
{

bool isAsterisk(const IAST & ast)
{
    return ast.as<ASTAsterisk>() || ast.as<ASTQualifiedAsterisk>()
        || ast.as<ASTColumnsListMatcher>() || ast.as<ASTColumnsRegexpMatcher>();
}

/// An asterisk selects every column, so the columns read cannot be enumerated from the AST. A
/// nested subquery carries its own select list, whose asterisks say nothing about this level.
bool selectsEverything(const IAST & ast)
{
    if (isAsterisk(ast))
        return true;
    if (ast.as<ASTSubquery>())
        return false;

    for (const auto & child : ast.children)
        if (child && selectsEverything(*child))
            return true;

    return false;
}

/// Walks a mutation expression and collects, by name, the reads it performs through a subquery, a
/// table on the right of `IN`, `dictGet` or `joinGet`. See `addExpressionIndirectReadsAccess`.
class IndirectReadsCollector
{
public:
    IndirectReadsCollector(AccessRightsElements & required_access_, ContextPtr context_)
        : required_access(required_access_)
        , context(std::move(context_))
    {
    }

    void visitExpression(const IAST * ast)
    {
        if (!ast)
            return;

        if (const auto * subquery = ast->as<ASTSubquery>())
        {
            visitSelectOrUnion(*subquery);
            return;
        }

        if (const auto * function = ast->as<ASTFunction>())
            visitNamedReads(*function);

        for (const auto & child : ast->children)
            visitExpression(child.get());
    }

private:
    /// The table an `IN`, `dictGet` or `joinGet` names in its arguments instead of reading it as a
    /// column. The column set of such a read is not enumerable from the AST except for `joinGet`,
    /// which names the one column it reads.
    void visitNamedReads(const ASTFunction & function)
    {
        if (!function.arguments)
            return;
        const auto & arguments = function.arguments->children;

        if (functionIsInOrGlobalInOperator(function.name) && arguments.size() == 2)
        {
            /// `x IN other` reads `other`; `x IN (SELECT ...)` and `x IN (1, 2)` do not name a table.
            if (auto table_id = tryGetNamedTable(*arguments[1]); table_id && !arguments[1]->as<ASTLiteral>())
                required_access.emplace_back(AccessType::SELECT, databaseOrCurrent(*table_id), table_id->table_name);
        }
        else if (functionIsJoinGet(function.name) && arguments.size() >= 2)
        {
            /// `joinGet('db.join_tbl', 'column', ...)` reads that one column, as checked by
            /// `FunctionJoinGet` itself when it is built - which for a mutation happens in the
            /// background, with full access.
            if (auto table_id = tryGetNamedTable(*arguments[0]))
            {
                const auto * column = arguments[1]->as<ASTLiteral>();
                if (column && column->value.getType() == Field::Types::String)
                    required_access.emplace_back(
                        AccessType::SELECT, databaseOrCurrent(*table_id), table_id->table_name,
                        Strings{column->value.safeGet<String>()});
                else
                    required_access.emplace_back(
                        AccessType::SELECT, databaseOrCurrent(*table_id), table_id->table_name);
            }
        }
        else if (functionIsDictGet(function.name) && !arguments.empty())
        {
            if (auto dictionary_id = tryGetNamedTable(*arguments[0]))
                required_access.emplace_back(
                    AccessType::dictGet, databaseOrCurrent(*dictionary_id), dictionary_id->table_name);
        }
    }

    /// A table named by an identifier (`x IN other`, `dictGet(db.dict, ...)`) or by a string
    /// literal (`joinGet('db.tbl', ...)`, `dictGet('db.dict', ...)`).
    static std::optional<StorageID> tryGetNamedTable(const IAST & argument)
    {
        if (const auto * identifier = argument.as<ASTIdentifier>())
        {
            /// Handles both a bare name and a compound `db.name`, and `ASTTableIdentifier` with it.
            DatabaseAndTableWithAlias database_and_table(*identifier);
            if (database_and_table.table.empty())
                return {};
            return StorageID{database_and_table.database, database_and_table.table};
        }

        const auto * literal = argument.as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::String)
            return {};

        const auto & name = literal->value.safeGet<String>();
        if (name.empty())
            return {};

        const auto dot = name.find('.');
        if (dot == String::npos)
            return StorageID{"", name};
        return StorageID{name.substr(0, dot), name.substr(dot + 1)};
    }

    void visitSelectOrUnion(const IAST & ast)
    {
        if (const auto * select = ast.as<ASTSelectQuery>())
        {
            visitSelect(*select);
            return;
        }

        /// `ASTSubquery` and `ASTSelectWithUnionQuery` hold the selects among their children; a
        /// union contributes every branch.
        for (const auto & child : ast.children)
            if (child)
                visitSelectOrUnion(*child);
    }

    void visitSelect(const ASTSelectQuery & select)
    {
        if (const auto with = select.with())
        {
            for (const auto & child : with->children)
            {
                if (const auto * with_element = child->as<ASTWithElement>())
                {
                    /// A `WITH` name is not a table to grant on, but its body reads tables.
                    cte_names.insert(with_element->name);
                    if (with_element->subquery)
                        visitSelectOrUnion(*with_element->subquery);
                }
                else
                {
                    visitExpression(child.get());
                }
            }
        }

        std::vector<StorageID> tables;
        String single_table_alias;
        /// Whether a column reference at this level can be attributed to one table.
        bool attributable = true;

        for (const auto * table_expression : getTableExpressions(select))
        {
            if (table_expression->subquery)
            {
                visitSelectOrUnion(*table_expression->subquery);
                attributable = false;
            }
            else if (table_expression->table_function)
            {
                /// Not covered - walk the arguments for nested subqueries at least.
                visitExpression(table_expression->table_function.get());
                attributable = false;
            }
            else if (const auto & name = table_expression->database_and_table_name)
            {
                const auto * identifier = name->as<ASTTableIdentifier>();
                if (!identifier)
                {
                    attributable = false;
                    continue;
                }

                auto table_id = identifier->getTableId();
                if (needsNoGrant(table_id))
                    continue;

                if (tables.empty())
                    single_table_alias = identifier->tryGetAlias();
                tables.push_back(std::move(table_id));
            }
        }

        const std::array expressions{
            select.select(), select.where(), select.prewhere(), select.having(), select.qualify(),
            select.groupBy(), select.orderBy(), select.limitBy()};

        /// Nested subqueries and named reads inside this level's expressions are reads of their own.
        for (const auto & expression : expressions)
            visitExpression(expression.get());

        if (tables.empty())
            return;

        if (attributable && tables.size() == 1)
        {
            if (auto columns = tryAttributeColumns(expressions, tables.front(), single_table_alias))
            {
                required_access.emplace_back(
                    AccessType::SELECT, databaseOrCurrent(tables.front()), tables.front().table_name, *columns);
                return;
            }
        }

        /// Fall back to the whole table, a superset of any column set it may read.
        for (const auto & table_id : tables)
            required_access.emplace_back(AccessType::SELECT, databaseOrCurrent(table_id), table_id.table_name);
    }

    /// The columns this level reads from its single table, or nothing when they cannot all be
    /// attributed to it.
    std::optional<Strings> tryAttributeColumns(
        const std::array<ASTPtr, 8> & expressions, const StorageID & table_id, const String & alias) const
    {
        RequiredSourceColumnsVisitor::Data columns_context;
        for (const auto & expression : expressions)
        {
            if (!expression)
                continue;
            if (selectsEverything(*expression))
                return {};

            auto expression_clone = expression->clone();
            RequiredSourceColumnsVisitor(columns_context).visit(expression_clone);
        }

        /// The qualifications a reference to this table may carry.
        Strings prefixes;
        if (!alias.empty())
            prefixes.emplace_back(alias + ".");
        prefixes.emplace_back(table_id.table_name + ".");
        if (!table_id.database_name.empty())
            prefixes.emplace_back(table_id.database_name + "." + table_id.table_name + ".");

        Strings columns;
        for (const auto & name : columns_context.requiredColumns())
        {
            std::string_view bare = name;
            for (const auto & prefix : prefixes)
            {
                if (bare.starts_with(prefix))
                {
                    bare.remove_prefix(prefix.size());
                    break;
                }
            }

            /// A name that is still dotted is either a column of a table this level does not name,
            /// or a real dotted column name - without the table's metadata the two are
            /// indistinguishable here, so stop attributing.
            if (bare.find('.') != std::string_view::npos)
                return {};

            columns.emplace_back(bare);
        }

        if (columns.empty())
            return {};

        return columns;
    }

    /// A `WITH` name and a session temporary table are not tables to grant `SELECT` on, exactly as
    /// in a plain `SELECT`.
    bool needsNoGrant(const StorageID & table_id) const
    {
        if (!table_id.database_name.empty())
            return false;

        return cte_names.contains(table_id.table_name)
            || static_cast<bool>(context->tryResolveStorageID(
                   StorageID{"", table_id.table_name}, Context::ResolveExternal));
    }

    String databaseOrCurrent(const StorageID & table_id) const
    {
        /// An empty database is kept when there is no current one: `executeDDLQueryOnCluster`
        /// expands an empty database in an access element to each host's default database, so the
        /// requirement travels with the query instead of being dropped.
        if (!table_id.database_name.empty())
            return table_id.database_name;
        return context->getCurrentDatabase();
    }

    AccessRightsElements & required_access;
    ContextPtr context;
    std::unordered_set<String> cte_names;
};

}

void addExpressionIndirectReadsAccess(
    AccessRightsElements & required_access,
    const IAST * expression,
    const ContextPtr & context)
{
    if (!expression)
        return;

    IndirectReadsCollector(required_access, context).visitExpression(expression);
}

}
