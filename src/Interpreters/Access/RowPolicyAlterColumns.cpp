#include <Interpreters/Access/RowPolicyAlterColumns.h>

#include <Access/AccessControl.h>
#include <Access/RowPolicy.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Interpreters/Context.h>
#include <Interpreters/RenameColumnVisitor.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/AlterCommands.h>
#include <base/range.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}

namespace
{

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

/// Don't go into subqueries: policies can't have correlated ones, so those columns are another table's.
void renameOutsideSubqueries(ASTPtr & ast, const RenameColumnData & rename)
{
    if (ast->as<ASTSubquery>())
        return;
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        rename.visit(*identifier, ast);
        return;
    }
    for (auto & child : ast->children)
        renameOutsideSubqueries(child, rename);
}

/// All policies that apply to this table and which columns they mention.
std::vector<BoundPolicy> collectBoundPolicies(const StorageID & table_id, const AccessControl & access_control)
{
    std::vector<BoundPolicy> result;
    for (const auto & id : access_control.findAll<RowPolicy>())
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy || policy->getDatabase() != table_id.getDatabaseName()
            || (!policy->isForDatabase() && policy->getTableName() != table_id.getTableName()))
            continue;

        BoundPolicy bound{id, policy, {}};
        for (const auto & filter : policy->filters)
        {
            if (filter.empty())
                continue;
            auto ast = parseFilter(filter, policy->getFullName().toString());
            RequiredSourceColumnsVisitor::Data columns_data;
            RequiredSourceColumnsVisitor(columns_data).visit(ast);
            const auto required = columns_data.requiredColumns();
            bound.columns.insert(required.begin(), required.end());
        }
        result.push_back(std::move(bound));
    }
    return result;
}

/// `ignore` is set by prepare() for IF EXISTS on a missing column, a no-op.
bool touchesColumns(const AlterCommand & command)
{
    return !command.ignore && (command.type == AlterCommand::DROP_COLUMN || command.type == AlterCommand::RENAME_COLUMN);
}

}

void checkRowPoliciesBeforeAlter(const StorageID & table_id, const AlterCommands & commands, const ContextPtr & context)
{
    if (std::none_of(commands.begin(), commands.end(), touchesColumns))
        return;

    const auto & access_control = context->getAccessControl();
    const auto bound_policies = collectBoundPolicies(table_id, access_control);

    for (const auto & command : commands)
    {
        if (!touchesColumns(command))
            continue;

        for (const auto & bound : bound_policies)
        {
            if (!bound.columns.contains(command.column_name))
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

void renameColumnsInRowPolicies(const StorageID & table_id, const AlterCommands & commands, const ContextPtr & context)
{
    std::vector<RenameColumnData> renames;
    for (const auto & command : commands)
        if (command.type == AlterCommand::RENAME_COLUMN && !command.ignore)
            renames.push_back({command.column_name, command.rename_to});
    if (renames.empty())
        return;

    /// The query context is const, but access entities are global anyway.
    auto & access_control = context->getGlobalContext()->getAccessControl();
    for (const auto & bound : collectBoundPolicies(table_id, access_control))
    {
        if (bound.policy->isForDatabase())
            continue;
        if (std::none_of(renames.begin(), renames.end(), [&](const auto & r) { return bound.columns.contains(r.column_name); }))
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
                for (const auto & rename : renames)
                    renameOutsideSubqueries(ast, rename);
                filter = ast->formatWithSecretsOneLine();
            }
            return updated;
        });
    }
}

}
