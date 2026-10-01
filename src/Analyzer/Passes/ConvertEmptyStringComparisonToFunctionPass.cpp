#include <Analyzer/Passes/ConvertEmptyStringComparisonToFunctionPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageProxy.h>
#include <Common/FieldVisitors.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_empty_string_comparisons;
}

namespace
{

bool isEmptyStringLiteral(const ASTPtr & ast)
{
    const auto * literal = ast->as<ASTLiteral>();
    return literal && literal->value.getType() == Field::Types::String && literal->value.safeGet<String>().empty();
}

bool comparesWithEmptyString(const ASTPtr & ast)
{
    if (const auto * function = ast->as<ASTFunction>();
        function && (function->name == "equals" || function->name == "notEquals") && function->arguments
        && function->arguments->children.size() == 2
        && (isEmptyStringLiteral(function->arguments->children[0]) || isEmptyStringLiteral(function->arguments->children[1])))
        return true;

    for (const auto & child : ast->children)
        if (comparesWithEmptyString(child))
            return true;
    return false;
}

bool keysDeclareComparisonWithEmptyString(const StorageInMemoryMetadata & metadata)
{
    for (const auto * key : {&metadata.getPartitionKey(), &metadata.getSortingKey(), &metadata.getPrimaryKey(), &metadata.getSamplingKey()})
        if (key->expression_list_ast && comparesWithEmptyString(key->expression_list_ast))
            return true;
    return false;
}

/// A key or skip index is matched to a query by its declared spelling, so a comparison with `''` in it must stay as written.
bool declaresComparisonWithEmptyString(const StorageInMemoryMetadata & metadata)
{
    if (keysDeclareComparisonWithEmptyString(metadata))
        return true;

    for (const auto & index : metadata.getSecondaryIndices())
        if (index.expression_list_ast && comparesWithEmptyString(index.expression_list_ast))
            return true;

    for (const auto & projection : metadata.getProjections())
        if (projection.metadata && keysDeclareComparisonWithEmptyString(*projection.metadata))
            return true;

    return false;
}

class FindTableWithIndexedEmptyStringComparisonVisitor : public InDepthQueryTreeVisitorWithContext<FindTableWithIndexedEmptyStringComparisonVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<FindTableWithIndexedEmptyStringComparisonVisitor>;
    using Base::Base;

    bool found = false;

    void enterImpl(QueryTreeNodePtr & node)
    {
        StoragePtr storage;
        if (const auto * table_node = node->as<TableNode>())
            storage = table_node->getStorage();
        else if (const auto * table_function_node = node->as<TableFunctionNode>())
            storage = table_function_node->getStorage();
        if (!storage)
            return;

        /// These wrappers forward `read` to a nested table with this query, so its indexes are the ones that matter.
        /// A `Distributed`, `Merge` or `View` table reads its tables with the already rewritten query and is not followed.
        for (size_t i = 0; storage && i < 16; ++i)
        {
            if (const auto * proxy = dynamic_cast<const StorageProxy *>(storage.get()))
                storage = proxy->getNested();
            else if (const auto * alias = storage->as<StorageAlias>())
                storage = alias->tryGetTargetTable();
            else if (const auto * materialized_view = storage->as<StorageMaterializedView>())
                storage = materialized_view->tryGetTargetTable();
            else if (const auto * buffer = storage->as<StorageBuffer>())
                storage = buffer->getDestinationTable();
            else
                break;
        }
        if (!storage)
            return;

        const auto metadata = storage->getInMemoryMetadataPtr(getContext(), false);
        if (declaresComparisonWithEmptyString(*metadata))
            found = true;
    }
};

class ConvertEmptyStringComparisonToFunctionVisitor
    : public InDepthQueryTreeVisitorWithContext<ConvertEmptyStringComparisonToFunctionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertEmptyStringComparisonToFunctionVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_empty_string_comparisons])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const String & func_name = function_node->getFunctionName();
        if (func_name != "equals" && func_name != "notEquals")
            return;

        const auto & args = function_node->getArguments().getNodes();
        if (args.size() != 2)
            return;

        // Identify which argument is the empty string literal
        int const_idx = -1;

        for (size_t i = 0; i < 2; ++i)
        {
            if (const auto * constant_node = args[i]->as<ConstantNode>())
            {
                if (isStringOrFixedString(constant_node->getResultType()))
                {
                    const Field & val = constant_node->getValue();
                    if (val.getType() == Field::Types::String && val.safeGet<String>().empty())
                    {
                        const_idx = static_cast<int>(i);
                        break;
                    }
                }
            }
        }

        if (const_idx == -1)
            return;

        size_t expr_idx = 1 - const_idx;
        const auto & expr_node = args[expr_idx];

        const auto expr_type = expr_node->getResultType();
        if (!expr_type || !isStringOrFixedString(expr_type))
            return;

        const String replacement_func = (func_name == "equals") ? "empty" : "notEmpty";

        auto replacement_node = std::make_shared<FunctionNode>(replacement_func);
        replacement_node->getArguments().getNodes().push_back(expr_node);

        resolveOrdinaryFunctionNodeByName(*replacement_node, replacement_func, getContext());

        node = std::move(replacement_node);
    }
};

}

void ConvertEmptyStringComparisonToFunctionPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    FindTableWithIndexedEmptyStringComparisonVisitor find_indexed_comparison(context);
    find_indexed_comparison.visit(query_tree_node);
    if (find_indexed_comparison.found)
        return;

    ConvertEmptyStringComparisonToFunctionVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
