#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/L2DistanceTransposedPartialReadsPass.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeQBit.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/IStorage.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Setting
{
extern const SettingsBool optimize_qbit_distance_function_reads;
}

namespace
{

class L2DistanceTransposedPartialReadsPassVisitor : public InDepthQueryTreeVisitorWithContext<L2DistanceTransposedPartialReadsPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<L2DistanceTransposedPartialReadsPassVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_qbit_distance_function_reads])
            return;

        /// Check if we can apply the optimization
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "L2DistanceTransposed")
            return;

        auto & function_arguments_nodes = function_node->getArguments().getNodes();
        if (function_arguments_nodes.size() != 3)
            return;

        auto * qbit_node = function_arguments_nodes[0]->as<ColumnNode>();
        auto ref_vec_node = function_arguments_nodes[1];
        auto * precision_node = function_arguments_nodes[2]->as<ConstantNode>();
        if (!qbit_node || qbit_node->getColumnName() == "__grouping_set" || !precision_node
            || precision_node->getValue().getType() != Field::Types::UInt64)
            return;

        auto column_source = qbit_node->getColumnSource();
        auto * table_node = column_source->as<TableNode>();
        if (!table_node)
            return;

        const auto & storage = table_node->getStorage();
        const auto & storage_snapshot = table_node->getStorageSnapshot();
        auto column_name_type = qbit_node->getColumn();

        if (!storage->supportsOptimizationToSubcolumns() || storage->isVirtualColumn(column_name_type.name, storage_snapshot->metadata))
            return;

        auto column_in_table = storage_snapshot->tryGetColumn(GetColumnsOptions::All, column_name_type.name);
        if (!column_in_table || !column_in_table->type->equals(*column_name_type.type))
            return;

        /// Apply the optimization
        const auto * qbit = checkAndGetDataType<DataTypeQBit>(qbit_node->getColumnType().get());

        if (!qbit)
            return;

        size_t data_width = qbit->getElementSize();
        UInt64 precision = precision_node->getValue().safeGet<UInt64>();

        if (precision == 0 || precision > data_width)
            return;

        std::vector<QueryTreeNodePtr> new_args;

        /// Create vec.1, vec.2, ..., vec.N components
        for (size_t i = 1; i <= precision; i++)
        {
            NameAndTypePair column{qbit_node->getColumnName() + "." + std::to_string(i), qbit->getNestedTupleElementType()};
            auto component_expr = std::make_shared<ColumnNode>(column, qbit_node->getColumnSource());
            new_args.push_back(component_expr);
        }

        /// Add dimension as penultimate argument and reference vector as last argument
        auto dimension_constant = std::make_shared<ConstantNode>(qbit->getDimension());

        /// If the precision node was nullable, the result needs to be nullable too. As this pass removes precision_node, we force
        /// the nullability on the dimension constant (if former was the case) to preserve the nullability of the result
        if (precision_node->getResultType()->isNullable() || precision_node->getResultType()->isLowCardinalityNullable())
            dimension_constant->convertToNullable();

        new_args.push_back(dimension_constant);
        new_args.push_back(ref_vec_node);

        auto original_result_type = function_node->getResultType();

        /// Re-resolve function with new arguments
        function_node->getArguments().getNodes() = std::move(new_args);
        auto function_builder = FunctionFactory::instance().get("L2DistanceTransposed", getContext());
        function_node->resolveAsFunction(function_builder->build(function_node->getArgumentColumns()));

        if (!function_node->getResultType()->equals(*original_result_type))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{} query tree node does not have a valid source node after running L2DistanceTransposedPartialReadsPass. Before: {}, "
                "after: {}",
                node->getNodeTypeName(),
                original_result_type->getName(),
                function_node->getResultType()->getName());
    }
};

}

void L2DistanceTransposedPartialReadsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    L2DistanceTransposedPartialReadsPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
