#include <Interpreters/Access/RowPolicyAlterColumns.h>

#include <Access/AccessControl.h>
#include <Access/RowPolicy.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Interpreters/Context.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}

namespace
{

/// Which part of an identifier names a column of the table: `t.tenant` -> {1, 1}, `j.user.name` -> {0, 1}, `n.x` -> {0, 2}.
struct ColumnRef
{
    size_t qualifier_parts;
    size_t column_parts;
    String column;
};

String joinParts(const std::vector<String> & parts, size_t from, size_t count)
{
    String result = parts[from];
    for (size_t i = from + 1; i < from + count; ++i)
        result += "." + parts[i];
    return result;
}

/// Same order as the analyzer: the whole name, then a column with a subcolumn, then after stripping `table.` or `db.table.`.
std::optional<ColumnRef> resolveColumn(const ASTIdentifier & identifier, const StorageID & table_id, const ColumnsDescription & columns)
{
    const auto & parts = identifier.name_parts;
    for (size_t skip : {0, 1, 2})
    {
        if (skip >= parts.size())
            break;
        if (skip == 1 && parts[0] != table_id.getTableName())
            continue;
        if (skip == 2 && (parts[0] != table_id.getDatabaseName() || parts[1] != table_id.getTableName()))
            continue;
        for (size_t len = parts.size() - skip; len > 0; --len)
        {
            auto name = joinParts(parts, skip, len);
            if (columns.has(name))
                return ColumnRef{skip, len, std::move(name)};
        }
    }
    return {};
}

/// Calls `on_column` for every identifier that names a column of the table.
struct ColumnRefWalker
{
    const StorageID & table_id;
    const ColumnsDescription & columns;
    std::function<void(ASTPtr &, const ColumnRef &)> on_column;

    void walk(ASTPtr & ast, const NameSet & shadowed)
    {
        /// Policies can't have correlated subqueries, so those columns are another table's.
        if (ast->as<ASTSubquery>())
            return;

        if (const auto * identifier = ast->as<ASTIdentifier>())
        {
            if (!shadowed.contains(identifier->name_parts.front()))
                if (auto ref = resolveColumn(*identifier, table_id, columns))
                    on_column(ast, *ref);
            return;
        }

        if (auto * function = ast->as<ASTFunction>(); function && function->arguments)
        {
            auto & args = function->arguments->children;
            if (function->name == "lambda" && args.size() == 2)
            {
                auto inner = shadowed;
                if (const auto * params = args[0]->as<ASTFunction>(); params && params->arguments)
                    for (const auto & param : params->arguments->children)
                        if (const auto * name = param->as<ASTIdentifier>())
                            inner.insert(name->name());
                walk(args[1], inner);
                return;
            }

            /// `x IN allowed`: the right side is a table.
            if (functionIsInOrGlobalInOperator(function->name) && args.size() == 2 && args[1]->as<ASTIdentifier>())
            {
                walk(args[0], shadowed);
                return;
            }
        }

        for (auto & child : ast->children)
            walk(child, shadowed);
    }
};

struct BoundPolicy
{
    UUID id;
    RowPolicyPtr policy;
    NameSet columns;
};

ASTPtr parseFilter(const String & filter, const String & policy_name)
{
    try
    {
        ParserExpression parser;
        return parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (Exception & e)
    {
        e.addMessage("while parsing the filter of row policy {}", policy_name);
        throw;
    }
}

/// All policies that apply to this table and which of its columns they read.
std::vector<BoundPolicy> collectBoundPolicies(const StorageID & table_id, const ColumnsDescription & columns, const AccessControl & access_control)
{
    std::vector<BoundPolicy> result;
    for (const auto & id : access_control.findAll<RowPolicy>())
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy || policy->getDatabase() != table_id.getDatabaseName()
            || (!policy->isForDatabase() && policy->getTableName() != table_id.getTableName()))
            continue;

        BoundPolicy bound{id, policy, {}};
        ColumnRefWalker walker{table_id, columns, [&](ASTPtr &, const ColumnRef & ref) { bound.columns.insert(ref.column); }};
        for (const auto & filter : policy->filters)
        {
            if (filter.empty())
                continue;
            auto ast = parseFilter(filter, policy->getFullName().toString());
            walker.walk(ast, {});
        }
        result.push_back(std::move(bound));
    }
    return result;
}

/// `n.x` is also a use of the Nested column `n`.
bool isColumnOrSubcolumnOf(const String & used, const String & column)
{
    return used == column || (used.starts_with(column) && used[column.size()] == '.');
}

bool usesColumn(const NameSet & used_columns, const String & column)
{
    return std::ranges::any_of(used_columns, [&](const auto & used) { return isColumnOrSubcolumnOf(used, column); });
}

/// `ignore` is set by prepare() for IF EXISTS on a missing column, a no-op.
bool touchesColumns(const AlterCommand & command)
{
    return !command.ignore && (command.type == AlterCommand::DROP_COLUMN || command.type == AlterCommand::RENAME_COLUMN);
}

}

void checkRowPoliciesBeforeAlter(
    const StorageID & table_id, const ColumnsDescription & columns, const AlterCommands & commands, const ContextPtr & context)
{
    if (std::none_of(commands.begin(), commands.end(), touchesColumns))
        return;

    const auto & access_control = context->getAccessControl();
    const auto bound_policies = collectBoundPolicies(table_id, columns, access_control);

    for (const auto & command : commands)
    {
        if (!touchesColumns(command))
            continue;

        for (const auto & bound : bound_policies)
        {
            if (!usesColumn(bound.columns, command.column_name))
                continue;

            const auto policy_name = bound.policy->getFullName().toString();
            if (command.type == AlterCommand::DROP_COLUMN)
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Cannot drop column {}: it is used by row policy {}", backQuote(command.column_name), policy_name);

            if (bound.policy->isForDatabase())
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Cannot rename column {}: it is used by row policy {}, which applies to every table of the database",
                    backQuote(command.column_name), policy_name);

            if (access_control.isReadOnly(bound.id))
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Cannot rename column {}: it is used by row policy {}, which is stored in a read-only access storage",
                    backQuote(command.column_name), policy_name);
        }
    }
}

void renameColumnsInRowPolicies(
    const StorageID & table_id, const ColumnsDescription & columns, const AlterCommands & commands, const ContextPtr & context)
{
    std::vector<std::pair<String, String>> renames;
    for (const auto & command : commands)
        if (command.type == AlterCommand::RENAME_COLUMN && !command.ignore)
            renames.emplace_back(command.column_name, command.rename_to);
    if (renames.empty())
        return;

    /// Replaces just the column in `t.tenant` or `j.user.name`, keeping the qualifier and the subcolumn.
    auto rename_identifier = [&](ASTPtr & node, const ColumnRef & ref)
    {
        for (const auto & [from, to] : renames)
        {
            if (!isColumnOrSubcolumnOf(ref.column, from))
                continue;
            const auto & old_parts = node->as<ASTIdentifier &>().name_parts;
            std::vector<String> parts(old_parts.begin(), old_parts.begin() + ref.qualifier_parts);
            parts.push_back(to + ref.column.substr(from.size()));
            parts.insert(parts.end(), old_parts.begin() + ref.qualifier_parts + ref.column_parts, old_parts.end());
            auto renamed = make_intrusive<ASTIdentifier>(std::move(parts));
            renamed->setAlias(node->tryGetAlias());
            node = renamed;
            return;
        }
    };

    /// The query context is const, but access entities are global anyway.
    auto & access_control = context->getGlobalContext()->getAccessControl();
    for (const auto & bound : collectBoundPolicies(table_id, columns, access_control))
    {
        if (bound.policy->isForDatabase())
            continue;
        if (std::none_of(renames.begin(), renames.end(), [&](const auto & r) { return usesColumn(bound.columns, r.first); }))
            continue;

        /// Another replica may have done it already, so start from the current version.
        access_control.update(bound.id, [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
        {
            auto updated = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
            for (auto & filter : updated->filters)
            {
                if (filter.empty())
                    continue;
                auto ast = parseFilter(filter, updated->getFullName().toString());
                ColumnRefWalker walker{table_id, columns, rename_identifier};
                walker.walk(ast, {});
                filter = ast->formatWithSecretsOneLine();
            }
            return updated;
        });
    }
}

}
