#include <Analyzer/Passes/NormalizeCountVariantsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_normalize_count_variants;
}

namespace
{

bool isNonNullableStoredColumn(const QueryTreeNodePtr & node, const ContextPtr & context)
{
    const auto * column_node = node->as<ColumnNode>();
    if (!column_node || column_node->hasExpression())
        return false;

    const auto & type = column_node->getColumnType();
    if (isNullableOrLowCardinalityNullable(type) || isVariant(type) || isDynamic(type) || isNothingOrNullableNothing(type))
        return false;

    /// Subquery and view outputs can require evaluating expressions that throw.
    /// Restrict this rewrite to ordinary stored `MergeTree` columns.
    const auto source = column_node->getColumnSourceOrNull();
    const auto * table_node = source ? source->as<TableNode>() : nullptr;
    const auto * merge_tree = table_node ? dynamic_cast<const MergeTreeData *>(table_node->getStorage().get()) : nullptr;
    if (!merge_tree)
        return false;

    const auto & snapshot = table_node->getStorageSnapshot();
    /// On-the-fly mutations can also evaluate expressions while reading a column.
    if (!table_node->getStorage()->supportsTrivialCountOptimization(snapshot, context))
        return false;

    const auto * column = snapshot->metadata->getColumns().tryGet(column_node->getColumnName());
    /// A column absent from older parts may evaluate its default expression on read.
    if (!column || column->default_desc.kind != ColumnDefaultKind::Default || column->default_desc.expression
        || !column->type->equals(*type))
        return false;

    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*snapshot->data);
    if (!snapshot_data.parts || !snapshot_data.mutations_snapshot
        || snapshot_data.mutations_snapshot->hasAlterMutations() || snapshot_data.mutations_snapshot->hasPatchParts()
        || merge_tree->hasEnabledMaskingPolicies(context))
        return false;

    /// Pending type changes can throw on read even with on-the-fly mutations disabled.
    /// Inspect the captured parts, since a mutation can finish after the snapshot was taken.
    for (const auto & part : *snapshot_data.parts)
    {
        const auto stored_column = part.data_part->getColumns().tryGetByName(column_node->getColumnName());
        if (!stored_column || !stored_column->type->equals(*type))
            return false;
    }

    return true;
}

class NormalizeCountVariantsVisitor : public InDepthQueryTreeVisitorWithContext<NormalizeCountVariantsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<NormalizeCountVariantsVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_normalize_count_variants])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction() || (function_node->getFunctionName() != "count" && function_node->getFunctionName() != "sum"))
            return;

        if (function_node->getArguments().getNodes().size() != 1)
            return;

        /// forbid the optimization if return value of sum() and count() differs:
        /// count() returns only UInt64 type, while sum() could return Nullable().
        if (!function_node->getResultType()->equals(DataTypeUInt64()))
            return;

        auto & first_argument = function_node->getArguments().getNodes()[0];
        auto * first_argument_constant_node = first_argument->as<ConstantNode>();
        if (function_node->getFunctionName() == "count"
            && ((first_argument_constant_node && !first_argument_constant_node->getValue().isNull())
                || isNonNullableStoredColumn(first_argument, getContext())))
        {
            function_node->getArguments().getNodes().clear();
            resolveAggregateFunctionNodeByName(*function_node, "count");
        }
        else if (function_node->getFunctionName() == "sum" && first_argument_constant_node &&
            first_argument_constant_node->getValue().getType() == Field::Types::UInt64 &&
            first_argument_constant_node->getValue().safeGet<UInt64>() == 1)
        {
            function_node->getArguments().getNodes().clear();
            resolveAggregateFunctionNodeByName(*function_node, "count");
        }
    }
};

}

void NormalizeCountVariantsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    NormalizeCountVariantsVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
