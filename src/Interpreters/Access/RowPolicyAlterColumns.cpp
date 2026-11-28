#include <Interpreters/Access/RowPolicyAlterColumns.h>

#include <Access/AccessControl.h>
#include <Access/RowPolicy.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/SetWithMemoryTracking.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageProxy.h>

#include <algorithm>
#include <tuple>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}

namespace
{

/// if exists on a missing column is a no-op (ignore), clear column keeps the column; a new type can break any filter
/// that reads the column, `n = 1` after `n String` or `tupleElement(tp, 'b')` after `tp Tuple(a UInt8)`, so it counts too
bool changesColumn(const AlterCommand & command, const ColumnsDescription & columns)
{
    if (command.ignore)
        return false;
    if (command.type == AlterCommand::MODIFY_COLUMN)
    {
        const auto * column = columns.tryGet(command.column_name);
        return command.data_type && column && !column->type->equals(*command.data_type);
    }
    return (command.type == AlterCommand::DROP_COLUMN && !command.clear) || command.type == AlterCommand::RENAME_COLUMN;
}

/// the storage columns a name reads when it resolves as written, like `tryResolveIdentifierFromStorage`: a column,
/// a subcolumn that the type of its column has, or the flattened `Nested` group n of the columns n.x and n.y
Names storageColumns(const String & name, const ColumnsDescription & columns)
{
    if (auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name))
        return {column->getNameInStorage()};
    Names children;
    if (!name.contains('.'))
        for (const auto & child : columns.getNested(name))
            children.push_back(child.name);
    return children;
}

/// the CTEs a `SELECT` level sees, by name, and if each reads a checked table, `c AS (SELECT ... FROM t)`
using Ctes = MapWithMemoryTracking<String, bool>;

/// what a `SELECT` level binds: the qualifiers of its names and the CTEs, so that `FROM c` reads t in the level and
/// in the subqueries inside it, and a CTE over another table hides the table of its name
struct Scope
{
    Strings qualifiers;
    Ctes ctes;

    auto operator<=>(const Scope &) const = default;
};

/// the identifiers a filter reads, SQL UDF bodies included, lambda parameters and the subqueries over other tables excluded
struct FilterNames
{
    const std::vector<StorageID> & tables;
    const ColumnsDescription & columns;
    /// each name as written, then without each qualifier of its level that it starts with
    SetWithMemoryTracking<Strings> names;
    /// a body is opened once per scope and per recording and reading state of its subquery
    SetWithMemoryTracking<std::tuple<String, Scope, bool, bool>> visited_udfs;

    /// an unqualified table binds in the database of the reader, so it counts in every database
    [[nodiscard]] static bool binds(const ASTTableIdentifier & table, const StorageID & checked)
    {
        auto id = table.getTableId();
        return checked.table_name == id.table_name && (id.database_name.empty() || checked.database_name == id.database_name);
    }

    [[nodiscard]] bool reads(const ASTTableIdentifier & table, const Ctes & ctes) const
    {
        if (!table.compound())
            if (auto cte = ctes.find(table.name()); cte != ctes.end())
                return cte->second;
        return std::ranges::any_of(tables, [&](const auto & checked) { return binds(table, checked); });
    }

    /// the tables of one subquery level are in its `FROM` and joins, a subquery in its expressions or `WITH` is
    /// a level of its own
    [[nodiscard]] bool readsCheckedTable(const IAST & ast, const Ctes & ctes) const
    {
        if (const auto * table = ast.as<ASTTableIdentifier>())
            return reads(*table, ctes);
        auto children_read = [&](const Ctes & level, bool from)
        {
            return std::ranges::any_of(ast.children, [&](const ASTPtr & child) { return (from || !child->as<ASTSubquery>()) && readsCheckedTable(*child, level); });
        };
        if (const auto * select = ast.as<ASTSelectQuery>())
            return children_read(levelCtes(*select, ctes), false);
        return children_read(ctes, ast.as<ASTTableExpression>() != nullptr);
    }

    /// a CTE of a `SELECT` hides the CTE or the table of the same name around it, except in its own body, and can
    /// read a CTE that comes after it
    [[nodiscard]] Ctes levelCtes(const ASTSelectQuery & select, const Ctes & outer) const
    {
        auto with = select.with();
        if (!with)
            return outer;
        Ctes level = outer;
        for (const auto & child : with->children)
            if (const auto * cte = child->as<ASTWithElement>())
                level[cte->name] = false;
        for (bool changed = true; changed;)
        {
            changed = false;
            for (const auto & child : with->children)
            {
                const auto * cte = child->as<ASTWithElement>();
                if (!cte || level.at(cte->name))
                    continue;
                if (auto own = outer.find(cte->name); own != outer.end())
                    level[cte->name] = own->second;
                else
                    level.erase(cte->name);
                const bool reading = readsCheckedTable(*cte->subquery, level);
                level[cte->name] = reading;
                changed = changed || reading;
            }
        }
        return level;
    }

    /// a table expression `... AS q` binds `q.` and a table t binds `t.` in its `SELECT` and in the subqueries inside,
    /// unless one of them binds the name again (see `tryBindIdentifierToTableExpression`), only a binding that reads
    /// a checked table qualifies its names
    [[nodiscard]] Scope levelScope(const ASTSelectQuery & select, const Scope & outer) const
    {
        const auto ctes = levelCtes(select, outer.ctes);
        Strings level;
        NameSet bound;
        if (auto from = select.tables())
        {
            for (const auto & element : from->children)
            {
                const auto & table_expression = element->as<ASTTablesInSelectQueryElement &>().table_expression;
                if (!table_expression)
                    continue;
                const bool reading = readsCheckedTable(*table_expression, ctes);
                const auto & expression = table_expression->as<ASTTableExpression &>();
                for (const auto & node : {expression.database_and_table_name, expression.table_function, expression.subquery})
                {
                    if (!node)
                        continue;
                    Strings bindings;
                    if (auto alias = node->tryGetAlias(); !alias.empty())
                        bindings.push_back(alias);
                    if (const auto * table = node->as<ASTTableIdentifier>())
                    {
                        bindings.push_back(table->shortName());
                        for (const auto & checked : tables)
                            if (reading && binds(*table, checked))
                                level.push_back(checked.database_name + "." + checked.table_name + ".");
                    }
                    for (const auto & name : bindings)
                        if (bound.insert(name).second && reading)
                            level.push_back(name + ".");
                }
            }
        }
        for (const auto & qualifier : outer.qualifiers)
            if (!bound.contains(qualifier.substr(0, qualifier.find('.'))))
                level.push_back(qualifier);
        return {std::move(level), ctes};
    }

    /// a filter cannot be correlated and cannot read its own table (see `buildFilterInfo`), so the names of a subquery
    /// level count only when the level or a level around it, to which it can be correlated, reads a checked table
    void collect(const ASTPtr & ast, const Scope & scope, NameSet bound = {}, bool record = true, bool in_reader = false)
    {
        if (const auto * select = ast->as<ASTSelectQuery>())
        {
            const auto level = levelScope(*select, scope);
            for (const auto & child : ast->children)
                collect(child, level, bound, record, in_reader);
            return;
        }
        if (ast->as<ASTSubquery>())
            record = in_reader = in_reader || readsCheckedTable(*ast, scope.ctes);
        if (ast->as<ASTTableIdentifier>())
            return;

        String name;
        if (const auto * identifier = ast->as<ASTIdentifier>())
        {
            name = identifier->name();
            if (std::ranges::any_of(bound, [&](const auto & parameter) { return name == parameter || name.starts_with(parameter + "."); }))
                return;
            if (record)
            {
                Strings candidates{name};
                Strings stripped_first;
                for (const auto & qualifier : scope.qualifiers)
                {
                    if (!name.starts_with(qualifier))
                        continue;
                    candidates.push_back(name.substr(qualifier.size()));
                    /// a reader with `analyzer_compatibility_prefer_alias_over_subcolumn` removes a table name or an alias first
                    if (std::ranges::count(qualifier, '.') == 1)
                        stripped_first.push_back(candidates.back());
                }
                if (!stripped_first.empty())
                {
                    stripped_first.insert(stripped_first.end(), candidates.begin(), candidates.end());
                    names.insert(std::move(stripped_first));
                }
                names.insert(std::move(candidates));
                /// a column takes priority over a SQL UDF of the same name, which a bare name calls only as a lambda, `arrayExists(udf, arr)`
                if (!storageColumns(name, columns).empty())
                    return;
            }
        }
        else if (const auto * function = ast->as<ASTFunction>())
        {
            name = function->name;
            /// x -> body is lambda(tuple(x), body), x is local to the body
            if (name == "lambda")
            {
                for (const auto & parameter : RequiredSourceColumnsMatcher::extractNamesFromLambda(*function))
                    bound.insert(parameter);
                collect(function->arguments->children[1], scope, bound, record, in_reader);
                return;
            }
        }
        /// a body can read a checked table in a subquery also where the caller does not, the name of the function is not a read
        if (auto body = UserDefinedSQLFunctionFactory::instance().tryGet(name); body && visited_udfs.emplace(name, scope, record, in_reader).second)
            collect(body->as<ASTCreateSQLFunctionQuery &>().function_core, scope, {}, record, in_reader);

        for (const auto & child : ast->children)
            collect(child, scope, bound, record, in_reader);
    }
};

/// candidates bind in their order, the name as written first and with a qualifier of its level removed only after
/// that (see `tryResolveIdentifierFromTableExpression`), a setting of the reader can change the order; an alter of
/// a column removes the column itself, or every child of a nested one when no column has that name (see `getNameRange`)
bool mentions(const Strings & candidates, const String & altered, const ColumnsDescription & columns)
{
    for (const auto & candidate : candidates)
    {
        auto read = storageColumns(candidate, columns);
        if (read.empty())
            continue;
        return std::ranges::any_of(read, [&](const auto & storage_column)
        {
            return columns.has(altered) ? storage_column == altered : storage_column.starts_with(altered + ".");
        });
    }
    return false;
}

/// a new name binds the names of a filter that start with it, `t.tenant` reads the subcolumn after `RENAME COLUMN payload TO t`
bool takes(const Strings & candidates, const String & new_name)
{
    return std::ranges::any_of(candidates, [&](const auto & candidate) { return candidate == new_name || candidate.starts_with(new_name + "."); });
}

/// a new type of a column also changes the `ALIAS` columns that read it: `a UInt8 ALIAS n = 1` fails after `n String`
Names changedColumns(const AlterCommand & command, const ColumnsDescription & columns)
{
    Names changed{command.column_name};
    const std::vector<StorageID> no_tables;
    for (size_t i = 0; command.type == AlterCommand::MODIFY_COLUMN && i < changed.size(); ++i)
    {
        for (const auto & column : columns)
        {
            if (column.default_desc.kind != ColumnDefaultKind::Alias || std::ranges::contains(changed, column.name))
                continue;
            FilterNames sources{no_tables, columns, {}, {}};
            sources.collect(column.default_desc.expression, {});
            if (std::ranges::any_of(sources.names, [&](const auto & candidates) { return mentions(candidates, changed[i], columns); }))
                changed.push_back(column.name);
        }
    }
    return changed;
}

/// a read through an `Alias` also applies the policies of its target chain (see `getRowPolicyFilterForStorage`),
/// so the policies of the table, of its chain, and of every alias that reaches them read the same columns
std::vector<StorageID> tablesSharingColumns(const StoragePtr & table, const ContextPtr & context)
{
    std::vector<StorageID> pending{table->getStorageID()};
    if (const auto * alias = resolveStorageProxyLoading(table)->as<StorageAlias>())
        for (const auto & [database, name] : alias->getTargetChain())
            pending.emplace_back(database, name);

    std::vector<StorageID> tables;
    auto & catalog = DatabaseCatalog::instance();
    while (!pending.empty())
    {
        auto id = std::move(pending.back());
        pending.pop_back();
        if (std::ranges::any_of(tables, [&](const auto & seen) { return seen.database_name == id.database_name && seen.table_name == id.table_name; }))
            continue;
        for (const auto & dependent : catalog.getReferentialDependents(id))
            if (auto storage = catalog.tryGetTable(dependent, context); storage && resolveStorageProxyLoading(storage)->as<StorageAlias>())
                pending.push_back(dependent);
        tables.push_back(std::move(id));
    }
    return tables;
}

}

void checkRowPoliciesBeforeAlter(const StoragePtr & table, const ColumnsDescription & columns, const AlterCommands & commands, const ContextPtr & context)
{
    /// row policies are local to a server, so a replica must apply what its initiator already committed
    auto changes = [&](const AlterCommand & command) { return changesColumn(command, columns); };
    if (isSecondaryDDLReplay(context) || std::ranges::none_of(commands, changes))
        return;

    const auto tables = tablesSharingColumns(table, context);
    const auto & access_control = context->getAccessControl();
    for (const auto & id : access_control.findAll<RowPolicy>())
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy)
            continue;
        /// outside subqueries a name is qualified by the table the policy is on, for `ON db.*` by each table of db
        for (const auto & candidate : tables)
        {
            if (candidate.database_name != policy->getDatabase() || (!policy->isForDatabase() && candidate.table_name != policy->getTableName()))
                continue;
            FilterNames filter_names{tables, columns, {}, {}};
            for (const auto & filter : policy->filters)
            {
                if (filter.empty())
                    continue;
                ParserExpression parser;
                filter_names.collect(parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                    {{candidate.table_name + ".", candidate.database_name + "." + candidate.table_name + "."}, {}});
            }

            for (const auto & command : commands)
            {
                if (!changes(command))
                    continue;
                if (command.type == AlterCommand::RENAME_COLUMN
                    && std::ranges::any_of(filter_names.names, [&](const auto & candidates) { return takes(candidates, command.rename_to); }))
                    throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Cannot rename column {} to {}: row policy {} mentions this name. Change or drop the policy first",
                        backQuote(command.column_name), backQuote(command.rename_to), policy->getFullName().toString());
                const auto changed = changedColumns(command, columns);
                if (std::ranges::none_of(filter_names.names, [&](const auto & candidates)
                    {
                        return std::ranges::any_of(changed, [&](const auto & column) { return mentions(candidates, column, columns); });
                    }))
                    continue;
                const char * action = command.type == AlterCommand::DROP_COLUMN ? "drop"
                    : command.type == AlterCommand::RENAME_COLUMN ? "rename" : "change the type of";
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Cannot {} column {}: row policy {} mentions it. Change or drop the policy first",
                    action, backQuote(command.column_name), policy->getFullName().toString());
            }
        }
    }
}

}
