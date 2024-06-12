#include <Storages/MergeTree/RPNBuilder.h>

#include <Common/FieldVisitorToString.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSubquery.h>

#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>

#include <Functions/IFunction.h>

#include <Storages/KeyDescription.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

void appendColumnNameWithoutAlias(const ActionsDAG::Node & node, WriteBuffer & out, bool allow_experimental_analyzer, bool legacy = false)
{
    switch (node.type)
    {
        case ActionsDAG::ActionType::INPUT:
            writeString(node.result_name, out);
            break;
        case ActionsDAG::ActionType::COLUMN:
        {
            /// If it was created from ASTLiteral, then result_name can be an alias.
            /// We need to convert value back to string here.
            const auto * column_const = typeid_cast<const ColumnConst *>(node.column.get());
            if (column_const && !allow_experimental_analyzer)
                writeString(applyVisitor(FieldVisitorToString(), column_const->getField()), out);
            else
                writeString(node.result_name, out);
            break;
        }
        case ActionsDAG::ActionType::ALIAS:
            appendColumnNameWithoutAlias(*node.children.front(), out, allow_experimental_analyzer, legacy);
            break;
        case ActionsDAG::ActionType::ARRAY_JOIN:
            writeCString("arrayJoin(", out);
            appendColumnNameWithoutAlias(*node.children.front(), out, allow_experimental_analyzer, legacy);
            writeChar(')', out);
            break;
        case ActionsDAG::ActionType::FUNCTION:
        {
            auto name = node.function_base->getName();
            if (legacy && name == "modulo")
                writeCString("moduloLegacy", out);
            else
                writeString(name, out);

            writeChar('(', out);
            bool first = true;
            for (const auto * arg : node.children)
            {
                if (!first)
                    writeCString(", ", out);
                first = false;

                appendColumnNameWithoutAlias(*arg, out, allow_experimental_analyzer, legacy);
            }
            writeChar(')', out);
        }
    }
}

String getColumnNameWithoutAlias(const ActionsDAG::Node & node, bool allow_experimental_analyzer, bool legacy = false)
{
    WriteBufferFromOwnString out;
    appendColumnNameWithoutAlias(node, out, allow_experimental_analyzer, legacy);

    return std::move(out.str());
}

const ActionsDAG::Node * getNodeWithoutAlias(const ActionsDAG::Node * node)
{
    const ActionsDAG::Node * result = node;

    while (result->type == ActionsDAG::ActionType::ALIAS)
        result = result->children[0];

    return result;
}

}

RPNBuilderTreeContext::RPNBuilderTreeContext(ContextPtr query_context_)
    : query_context(std::move(query_context_))
{}

RPNBuilderTreeContext::RPNBuilderTreeContext(ContextPtr query_context_, Block block_with_constants_, PreparedSetsPtr prepared_sets_)
    : query_context(std::move(query_context_))
    , block_with_constants(std::move(block_with_constants_))
    , prepared_sets(std::move(prepared_sets_))
{}

RPNBuilderTreeNode::RPNBuilderTreeNode(const ActionsDAG::Node * dag_node_, RPNBuilderTreeContext & tree_context_)
    : dag_node(dag_node_)
    , tree_context(tree_context_)
{
    assert(dag_node);
}

RPNBuilderTreeNode::RPNBuilderTreeNode(const IAST * ast_node_, RPNBuilderTreeContext & tree_context_)
    : ast_node(ast_node_)
    , tree_context(tree_context_)
{
    assert(ast_node);
}

std::string RPNBuilderTreeNode::getColumnName() const
{
    if (ast_node)
        return ast_node->getColumnNameWithoutAlias();
    else
        return getColumnNameWithoutAlias(*dag_node, getTreeContext().getSettings().allow_experimental_analyzer);
}

std::string RPNBuilderTreeNode::getColumnNameWithModuloLegacy() const
{
    if (ast_node)
    {
        auto adjusted_ast = ast_node->clone();
        KeyDescription::moduloToModuloLegacyRecursive(adjusted_ast);
        return adjusted_ast->getColumnNameWithoutAlias();
    }
    else
    {
        return getColumnNameWithoutAlias(*dag_node, getTreeContext().getSettings().allow_experimental_analyzer, true /*legacy*/);
    }
}

bool RPNBuilderTreeNode::isFunction() const
{
    if (ast_node)
    {
        return typeid_cast<const ASTFunction *>(ast_node);
    }
    else
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);
        return node_without_alias->type == ActionsDAG::ActionType::FUNCTION;
    }
}

bool RPNBuilderTreeNode::isConstant() const
{
    if (ast_node)
    {
        bool is_literal = typeid_cast<const ASTLiteral *>(ast_node);
        if (is_literal)
            return true;

        String column_name = ast_node->getColumnName();
        const auto & block_with_constants = tree_context.getBlockWithConstants();

        if (block_with_constants.has(column_name) && isColumnConst(*block_with_constants.getByName(column_name).column))
            return true;

        return false;
    }
    else
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);
        return node_without_alias->column && isColumnConst(*node_without_alias->column);
    }
}

bool RPNBuilderTreeNode::isSubqueryOrSet() const
{
    if (ast_node)
    {
        return
            typeid_cast<const ASTSubquery *>(ast_node) ||
            typeid_cast<const ASTTableIdentifier *>(ast_node);
    }
    else
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);
        return node_without_alias->result_type->getTypeId() == TypeIndex::Set;
    }
}

ColumnWithTypeAndName RPNBuilderTreeNode::getConstantColumn() const
{
    if (!isConstant())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RPNBuilderTree node is not a constant");

    ColumnWithTypeAndName result;

    if (ast_node)
    {
        const auto * literal = typeid_cast<const ASTLiteral *>(ast_node);
        if (literal)
        {
            result.type = applyVisitor(FieldToDataType(), literal->value);
            result.column = result.type->createColumnConst(0, literal->value);

            return result;
        }

        String column_name = ast_node->getColumnName();
        const auto & block_with_constants = tree_context.getBlockWithConstants();

        return block_with_constants.getByName(column_name);
    }
    else
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);
        result.type = node_without_alias->result_type;
        result.column = node_without_alias->column;
    }

    return result;
}

bool RPNBuilderTreeNode::tryGetConstant(Field & output_value, DataTypePtr & output_type) const
{
    if (ast_node)
    {
        // Constant expr should use alias names if any
        String column_name = ast_node->getColumnName();
        const auto & block_with_constants = tree_context.getBlockWithConstants();

        if (const auto * literal = ast_node->as<ASTLiteral>())
        {
            /// By default block_with_constants has only one column named "_dummy".
            /// If block contains only constants it's may not be preprocessed by
            //  ExpressionAnalyzer, so try to look up in the default column.
            if (!block_with_constants.has(column_name))
                column_name = "_dummy";

            /// Simple literal
            output_value = literal->value;
            output_type = block_with_constants.getByName(column_name).type;

            /// If constant is not Null, we can assume it's type is not Nullable as well.
            if (!output_value.isNull())
                output_type = removeNullable(output_type);

            return true;
        }
        else if (block_with_constants.has(column_name) &&
            isColumnConst(*block_with_constants.getByName(column_name).column))
        {
            /// An expression which is dependent on constants only
            const auto & constant_column = block_with_constants.getByName(column_name);
            output_value = (*constant_column.column)[0];
            output_type = constant_column.type;

            if (!output_value.isNull())
                output_type = removeNullable(output_type);

            return true;
        }
    }
    else
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);

        if (node_without_alias->column && isColumnConst(*node_without_alias->column))
        {
            output_value = (*node_without_alias->column)[0];
            output_type = node_without_alias->result_type;

            if (!output_value.isNull())
                output_type = removeNullable(output_type);

            return true;
        }
    }

    return false;
}

namespace
{

FutureSetPtr tryGetSetFromDAGNode(const ActionsDAG::Node * dag_node)
{
    if (!dag_node->column)
        return {};

    const IColumn * column = dag_node->column.get();
    if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
        column = &column_const->getDataColumn();

    if (const auto * column_set = typeid_cast<const ColumnSet *>(column))
        return column_set->getData();

    return {};
}

}

FutureSetPtr RPNBuilderTreeNode::tryGetPreparedSet() const
{
    const auto & prepared_sets = getTreeContext().getPreparedSets();

    if (ast_node && prepared_sets)
    {
        auto key = ast_node->getTreeHash();
        const auto & sets = prepared_sets->getSetsFromTuple();
        auto it = sets.find(key);
        if (it != sets.end() && !it->second.empty())
            return it->second.at(0);

        return prepared_sets->findSubquery(key);
    }
    else if (dag_node)
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);
        return tryGetSetFromDAGNode(node_without_alias);
    }

    return {};
}

FutureSetPtr RPNBuilderTreeNode::tryGetPreparedSet(const DataTypes & data_types) const
{
    const auto & prepared_sets = getTreeContext().getPreparedSets();

    if (prepared_sets && ast_node)
    {
        if (ast_node->as<ASTSubquery>() || ast_node->as<ASTTableIdentifier>())
            return prepared_sets->findSubquery(ast_node->getTreeHash());

        return prepared_sets->findTuple(ast_node->getTreeHash(), data_types);
    }
    else if (dag_node)
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);
        return tryGetSetFromDAGNode(node_without_alias);
    }

    return nullptr;
}

FutureSetPtr RPNBuilderTreeNode::tryGetPreparedSet(
    const std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> & indexes_mapping,
    const DataTypes & data_types) const
{
    const auto & prepared_sets = getTreeContext().getPreparedSets();

    /// We have `PreparedSetKey::forLiteral` but it is useless here as we don't have enough information
    /// about types in left argument of the IN operator. Instead, we manually iterate through all the sets
    /// and find the one for the right arg based on the AST structure (getTreeHash), after that we check
    /// that the types it was prepared with are compatible with the types of the primary key.
    auto types_match = [&indexes_mapping, &data_types](const DataTypes & set_types)
    {
        assert(indexes_mapping.size() == data_types.size());

        for (size_t i = 0; i < indexes_mapping.size(); ++i)
        {
            if (indexes_mapping[i].tuple_index >= set_types.size())
                return false;

            auto lhs = removeNullable(recursiveRemoveLowCardinality(data_types[i]));
            auto rhs = removeNullable(recursiveRemoveLowCardinality(set_types[indexes_mapping[i].tuple_index]));

            if (!lhs->equals(*rhs))
                return false;
        }

        return true;
    };

    if (prepared_sets && ast_node)
    {
        if (ast_node->as<ASTSubquery>() || ast_node->as<ASTTableIdentifier>())
            return prepared_sets->findSubquery(ast_node->getTreeHash());

        auto tree_hash = ast_node->getTreeHash();
        const auto & sets = prepared_sets->getSetsFromTuple();
        auto it = sets.find(tree_hash);
        if (it == sets.end())
            return nullptr;

        for (const auto & future_set : it->second)
            if (types_match(future_set->getTypes()))
                return future_set;
    }
    else
    {
        const auto * node_without_alias = getNodeWithoutAlias(dag_node);
        if (node_without_alias->column)
        {
            auto future_set = tryGetSetFromDAGNode(node_without_alias);
            if (types_match(future_set->getTypes()))
                return future_set;
        }
    }

    return nullptr;
}

RPNBuilderFunctionTreeNode RPNBuilderTreeNode::toFunctionNode() const
{
    if (!isFunction())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RPNBuilderTree node is not a function");

    if (ast_node)
        return RPNBuilderFunctionTreeNode(ast_node, tree_context);
    else
        return RPNBuilderFunctionTreeNode(getNodeWithoutAlias(dag_node), tree_context);
}

std::optional<RPNBuilderFunctionTreeNode> RPNBuilderTreeNode::toFunctionNodeOrNull() const
{
    if (!isFunction())
        return {};

    if (ast_node)
        return RPNBuilderFunctionTreeNode(this->ast_node, tree_context);
    else
        return RPNBuilderFunctionTreeNode(getNodeWithoutAlias(dag_node), tree_context);
}

std::string RPNBuilderFunctionTreeNode::getFunctionName() const
{
    if (ast_node)
        return assert_cast<const ASTFunction *>(ast_node)->name;
    else
        return dag_node->function_base->getName();
}

size_t RPNBuilderFunctionTreeNode::getArgumentsSize() const
{
    if (ast_node)
    {
        const auto * ast_function = assert_cast<const ASTFunction *>(ast_node);
        return ast_function->arguments ? ast_function->arguments->children.size() : 0;
    }
    else
    {
        return dag_node->children.size();
    }
}

RPNBuilderTreeNode RPNBuilderFunctionTreeNode::getArgumentAt(size_t index) const
{
    if (ast_node)
    {
        const auto * ast_function = assert_cast<const ASTFunction *>(ast_node);
        return RPNBuilderTreeNode(ast_function->arguments->children[index].get(), tree_context);
    }
    else
    {
        return RPNBuilderTreeNode(dag_node->children[index], tree_context);
    }
}

}
