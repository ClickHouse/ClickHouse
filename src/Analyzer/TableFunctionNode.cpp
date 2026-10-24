#include <Analyzer/TableFunctionNode.h>

#include <Analyzer/ConstantNode.h>

#include <Common/assert_cast.h>
#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Storages/IStorage.h>
#include <Storages/StorageView.h>

#include <TableFunctions/ITableFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// An unqualified parameterized-view name re-resolves against the receiving server's default
/// database, so it is sent qualified. Only a 2-part result is resolvable as a parameterized view,
/// so a dotted database name is left alone.
bool isSentQualified(const TableFunctionNode & node)
{
    if (!node.isParameterizedView() || node.getTableFunctionName().contains('.'))
        return false;

    const auto & storage_id = node.getStorageID();
    return storage_id.hasDatabase() && !storage_id.getDatabaseName().contains('.');
}

}

TableFunctionNode::TableFunctionNode(String table_function_name_)
    : ITableExpressionNode(children_size)
    , table_function_name(table_function_name_)
    , storage_id("system", "one")
{
    children[arguments_child_index] = std::make_shared<ListNode>();
}

void TableFunctionNode::resolve(TableFunctionPtr table_function_value, StoragePtr storage_value, ContextPtr context, VectorWithMemoryTracking<size_t> unresolved_arguments_indexes_)
{
    table_function = std::move(table_function_value);
    storage = std::move(storage_value);
    storage_id = storage->getStorageID();
    /// A parameterized view is resolved as a table function node but has no `ITableFunction` object
    /// (see `QueryAnalyzer::resolveTableFunction`), and it references no table of its own.
    referenced_table_id = table_function ? table_function->getReferencedTableID() : StorageID::createEmpty();
    evaluates_arguments_remotely = table_function && table_function->evaluatesArgumentsRemotely();
    if (auto table_argument = table_function ? table_function->getTableArgument() : std::nullopt)
    {
        table_argument_id = std::move(table_argument->table_id);
        num_table_arguments = table_argument->num_arguments;
    }
    unresolved_arguments_indexes = std::move(unresolved_arguments_indexes_);

    const auto metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);
    storage_snapshot = storage->getStorageSnapshot(metadata_snapshot, context);

    if (table_expression_modifiers)
        storage_snapshot = storage_snapshot->clone(extendMetadataWithModifiers(storage_snapshot->metadata, *table_expression_modifiers), storage_snapshot->data);
}

void TableFunctionNode::setTableExpressionModifiers(TableExpressionModifiers table_expression_modifiers_value)
{
    table_expression_modifiers = std::move(table_expression_modifiers_value);

    if (storage_snapshot)
        storage_snapshot = storage_snapshot->clone(extendMetadataWithModifiers(storage_snapshot->metadata, *table_expression_modifiers), storage_snapshot->data);
}

const StorageID & TableFunctionNode::getStorageID() const
{
    if (!storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function node {} is not resolved", table_function_name);

    return storage_id;
}

const StorageSnapshotPtr & TableFunctionNode::getStorageSnapshot() const
{
    if (!storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function node {} is not resolved", table_function_name);

    return storage_snapshot;
}

bool TableFunctionNode::isParameterizedView() const
{
    const auto * storage_view = storage ? storage->as<StorageView>() : nullptr;
    return storage_view && storage_view->isParameterizedView();
}

void TableFunctionNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "TABLE_FUNCTION id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", table_function_name: " << table_function_name;

    if (table_expression_modifiers)
    {
        buffer << ", ";
        table_expression_modifiers->dump(buffer);
    }

    const auto & arguments = getArguments();
    if (!arguments.getNodes().empty())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "ARGUMENTS\n";
        arguments.dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (!settings_changes.empty())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "SETTINGS";
        for (const auto & change : settings_changes)
            buffer << fmt::format(" {}={}", change.name, fieldToString(change.value));
    }
}

bool TableFunctionNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const TableFunctionNode &>(rhs);

    /// A parameterized view is identified by its storage, whether it is spelled `pv` or `db.pv`.
    const bool both_parameterized_views = isParameterizedView() && rhs_typed.isParameterizedView();
    if (!both_parameterized_views && table_function_name != rhs_typed.table_function_name)
        return false;

    if (storage && rhs_typed.storage && storage_id != rhs_typed.storage_id)
        return false;

    if (settings_changes != rhs_typed.settings_changes)
        return false;

    return table_expression_modifiers == rhs_typed.table_expression_modifiers;
}

void TableFunctionNode::updateTreeHashImpl(HashState & state, CompareOptions) const
{
    const auto full_name = storage ? storage_id.getFullNameNotQuoted() : String{};

    /// Hash the name `toASTImpl` sends to other servers: `IN` set names derive from this hash.
    const auto & name = isSentQualified(*this) ? full_name : table_function_name;
    state.update(name.size());
    state.update(name);

    if (storage)
    {
        state.update(full_name.size());
        state.update(full_name);
    }

    if (table_expression_modifiers)
        table_expression_modifiers->updateTreeHash(state);

    state.update(settings_changes.size());
    for (const auto & change : settings_changes)
    {
        state.update(change.name.size());
        state.update(change.name);
        state.update(change.shorthand);

        const auto & value_dump = change.value.dump();
        state.update(value_dump.size());
        state.update(value_dump);
    }
}

QueryTreeNodePtr TableFunctionNode::cloneImpl() const
{
    auto result = std::make_shared<TableFunctionNode>(table_function_name);

    result->storage = storage;
    result->storage_id = storage_id;
    result->referenced_table_id = referenced_table_id;
    result->table_argument_id = table_argument_id;
    result->num_table_arguments = num_table_arguments;
    result->evaluates_arguments_remotely = evaluates_arguments_remotely;
    result->storage_snapshot = storage_snapshot;
    result->table_expression_modifiers = table_expression_modifiers;
    result->settings_changes = settings_changes;
    result->unresolved_arguments_indexes = unresolved_arguments_indexes;

    return result;
}

void TableFunctionNode::qualifyTableArgument()
{
    if (num_table_arguments == 0 || !table_argument_id.hasDatabase())
        return;

    auto & arguments = getArguments().getNodes();
    if (arguments.size() < num_table_arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function {} has {} arguments, but its table is named by {}",
            table_function_name, arguments.size(), num_table_arguments);

    constexpr size_t num_qualified_arguments = 2;
    arguments.erase(arguments.begin(), arguments.begin() + num_table_arguments);
    arguments.insert(arguments.begin(), std::make_shared<ConstantNode>(table_argument_id.getTableName()));
    arguments.insert(arguments.begin(), std::make_shared<ConstantNode>(table_argument_id.getDatabaseName()));

    /// The indexes of the arguments that follow the table move with them.
    VectorWithMemoryTracking<size_t> new_unresolved_arguments_indexes;
    for (size_t index : unresolved_arguments_indexes)
        if (index >= num_table_arguments)
            new_unresolved_arguments_indexes.push_back(index - num_table_arguments + num_qualified_arguments);
    unresolved_arguments_indexes = std::move(new_unresolved_arguments_indexes);

    num_table_arguments = num_qualified_arguments;
}

ASTPtr TableFunctionNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto table_function_ast = make_intrusive<ASTFunction>();

    table_function_ast->name = isSentQualified(*this) ? storage_id.getFullNameNotQuoted() : table_function_name;

    const auto & arguments = getArguments();
    table_function_ast->children.push_back(arguments.toAST(options));
    table_function_ast->arguments = table_function_ast->children.back();

    if (!settings_changes.empty())
    {
        auto settings_ast = make_intrusive<ASTSetQuery>();
        settings_ast->changes = settings_changes;
        settings_ast->is_standalone = false;
        table_function_ast->arguments->children.push_back(std::move(settings_ast));
    }

    return table_function_ast;
}

}
