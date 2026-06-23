#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/DistanceTransposedPartialReadsPass.h>
#include <Analyzer/Utils.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
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

/// The reference vector is referenced more than once in the rewrite (the null mask, the
/// `assumeNotNull` value and the NULL-row dummy guard). Reusing the expression node is only
/// correct when it yields the same value at every reference, i.e. when it is deterministic.
bool isDeterministicExpression(const QueryTreeNodePtr & node)
{
    QueryTreeNodes nodes_to_process{node};
    while (!nodes_to_process.empty())
    {
        auto current = nodes_to_process.back();
        nodes_to_process.pop_back();

        switch (current->getNodeType())
        {
            case QueryTreeNodeType::FUNCTION:
            {
                const auto * function_node = current->as<FunctionNode>();
                const auto & function_base = function_node->getFunction();
                if (!function_base || !function_base->isDeterministicInScopeOfQuery())
                    return false;
                for (const auto & argument : function_node->getArguments().getNodes())
                    nodes_to_process.push_back(argument);
                break;
            }
            case QueryTreeNodeType::CONSTANT:
            {
                if (!current->as<ConstantNode>()->isDeterministic())
                    return false;
                break;
            }
            case QueryTreeNodeType::COLUMN:
                break;
            default:
                return false;
        }
    }
    return true;
}

class DistanceTransposedPartialReadsPassVisitor : public InDepthQueryTreeVisitorWithContext<DistanceTransposedPartialReadsPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<DistanceTransposedPartialReadsPassVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_qbit_distance_function_reads])
            return;

        /// Check if we can apply the optimization
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const auto & function_name = function_node->getFunctionName();
        bool is_distance_function = (function_name == "L2DistanceTransposed" || function_name == "cosineDistanceTransposed");

        if (!is_distance_function)
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

        if (!storage->supportsOptimizationToSubcolumns() || storage_snapshot->metadata->isVirtualColumn(column_name_type.name))
            return;

        auto column_in_table = storage_snapshot->tryGetColumn(GetColumnsOptions::All, column_name_type.name);
        if (!column_in_table || !column_in_table->type->equals(*column_name_type.type))
            return;

        /// The rewrite casts the reference vector to a non-nullable `Array(element_type)`, which
        /// drops the row null mask it carries (Nullable / LowCardinalityNullable / Variant / Dynamic).
        /// Left as is, that turns the result from Nullable(Float64) into Float64. We still apply the
        /// optimization for such reference vectors but reinstate the mask around the rewritten call
        /// (the `ref_vec_is_nullable` branches below). An always-NULL reference (Nullable(Nothing))
        /// carries no element type to cast to and yields a NULL distance on every row, so skip it.
        const auto & ref_vec_type = ref_vec_node->getResultType();
        if (ref_vec_type->onlyNull())
            return;
        const bool ref_vec_is_nullable
            = ref_vec_type->isNullable() || ref_vec_type->isLowCardinalityNullable() || isVariant(ref_vec_type)
            || isDynamic(ref_vec_type);

        /// The mask-lifting rewrite below evaluates the reference expression at several places
        /// (the null mask, the `assumeNotNull` value and the NULL-row dummy guard). For a
        /// non-deterministic reference (e.g. `if(rand() % 2, [...], NULL)`) those evaluations
        /// could sample different values, so skip the optimization there and read the whole column.
        if (ref_vec_is_nullable && !isDeterministicExpression(ref_vec_node))
            return;

        /// Apply the optimization
        const auto * qbit = checkAndGetDataType<DataTypeQBit>(qbit_node->getColumnType().get());

        if (!qbit)
            return;

        size_t data_width = qbit->getElementSize();
        UInt64 precision = precision_node->getValue().safeGet<UInt64>();

        if (precision == 0 || precision > data_width)
            return;

        auto original_result_type = function_node->getResultType();

        auto make_resolved = [&](const String & name, QueryTreeNodes args) -> QueryTreeNodePtr
        {
            auto fn = std::make_shared<FunctionNode>(name);
            fn->getArguments().getNodes() = std::move(args);
            resolveOrdinaryFunctionNodeByName(*fn, name, getContext());
            return fn;
        };

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

        /// Cast reference vector to match QBit type. This is the only information about the type of the QBit after this pass is applied
        auto expected_ref_vec_type = std::make_shared<DataTypeArray>(qbit->getElementType());

        auto cast_to_expected_type = [&](QueryTreeNodePtr ref) -> QueryTreeNodePtr
        {
            if (ref->getResultType()->equals(*expected_ref_vec_type))
                return ref;
            return make_resolved("_CAST", {ref, std::make_shared<ConstantNode>(expected_ref_vec_type->getName())});
        };

        if (ref_vec_is_nullable)
        {
            /// Casting the reference vector to a non-nullable `Array` drops its row null mask. Keep reading the
            /// subcolumns, but feed the call `assumeNotNull(ref)` cast to the expected type and reinstate the mask
            /// around the result. On a NULL row `assumeNotNull` yields a wrong-sized array, so substitute a
            /// correctly sized dummy there: that keeps the rewritten call independent of short-circuit evaluation
            /// (which CI randomizes) while still surfacing a genuine wrong-size error for non-null references.
            Array dummy_ref_vec(qbit->getDimension(), Field(0));
            auto dummy_ref_node = std::make_shared<ConstantNode>(Field(std::move(dummy_ref_vec)), expected_ref_vec_type);

            auto ref_is_null = make_resolved("isNull", {ref_vec_node->clone()});
            auto not_null_ref = cast_to_expected_type(make_resolved("assumeNotNull", {ref_vec_node->clone()}));
            new_args.push_back(make_resolved("if", {ref_is_null, dummy_ref_node, not_null_ref}));
        }
        else
        {
            new_args.push_back(cast_to_expected_type(ref_vec_node));
        }

        /// Re-resolve function with new arguments
        function_node->getArguments().getNodes() = std::move(new_args);
        auto function_builder = FunctionFactory::instance().get(function_name, getContext());
        function_node->resolveAsFunction(function_builder->build(function_node->getArgumentColumns()));

        if (ref_vec_is_nullable)
        {
            /// The rewritten call returns a non-nullable `Float64`; reinstate the original row null mask so the
            /// result type matches what the user wrote (`Nullable(Float64)`).
            auto distance_node = node;
            auto null_const = std::make_shared<ConstantNode>(Field{}, original_result_type);
            node = make_resolved("if", {make_resolved("isNull", {ref_vec_node->clone()}), null_const, distance_node});
        }

        if (!node->getResultType()->equals(*original_result_type))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{} query tree node does not have a valid source node after running DistanceTransposedPartialReadsPass. Before: {}, "
                "after: {}",
                node->getNodeTypeName(),
                original_result_type->getName(),
                node->getResultType()->getName());
    }
};

}

void DistanceTransposedPartialReadsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    DistanceTransposedPartialReadsPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
