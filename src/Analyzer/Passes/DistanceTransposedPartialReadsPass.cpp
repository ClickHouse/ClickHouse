#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/DistanceTransposedPartialReadsPass.h>
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
        bool is_distance_function
            = (function_name == "L2DistanceTransposed" || function_name == "cosineDistanceTransposed"
               || function_name == "dotProductTransposed");

        if (!is_distance_function)
            return;

        auto & function_arguments_nodes = function_node->getArguments().getNodes();
        if (function_arguments_nodes.size() != 3 && function_arguments_nodes.size() != 4)
            return;

        auto * qbit_node = function_arguments_nodes[0]->as<ColumnNode>();
        auto ref_vec_node = function_arguments_nodes[1];
        auto * precision_node = function_arguments_nodes[2]->as<ConstantNode>();
        if (!qbit_node || qbit_node->getColumnName() == "__grouping_set" || !precision_node
            || precision_node->getValue().getType() != Field::Types::UInt64)
            return;

        /// Optional fourth argument: the number of dimensions to read (Matryoshka-style partial-dimension search).
        const ConstantNode * used_dims_node = nullptr;
        if (function_arguments_nodes.size() == 4)
        {
            used_dims_node = function_arguments_nodes[3]->as<ConstantNode>();
            if (!used_dims_node || used_dims_node->getValue().getType() != Field::Types::UInt64)
                return;
        }

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

        /// If the function result type is Nullable(Nothing), skip the optimization.
        /// This happens when some arguments are NULL constants (e.g. from fuzzer),
        /// and rewriting the function with cast arguments would change the result type.
        if (function_node->getResultType()->onlyNull())
            return;

        /// Apply the optimization
        const auto * qbit = checkAndGetDataType<DataTypeQBit>(qbit_node->getColumnType().get());

        if (!qbit)
            return;

        size_t data_width = qbit->getElementSize();
        UInt64 precision = precision_node->getValue().safeGet<UInt64>();

        if (precision == 0 || precision > data_width)
            return;

        const size_t element_size = qbit->getElementSize();
        const size_t stride = qbit->getStride();
        const size_t dimension = qbit->getDimension();
        const bool is_strided = qbit->getNumStrides() > 1;

        /// Number of dimensions to read. Defaults to the full dimension when the optional 4th argument is absent.
        UInt64 used_dims = dimension;
        if (used_dims_node)
        {
            used_dims = used_dims_node->getValue().safeGet<UInt64>();
            if (used_dims == 0 || used_dims > dimension || used_dims % stride != 0)
                return;
        }

        std::vector<QueryTreeNodePtr> new_args;

        auto add_plane = [&](size_t tuple_idx)
        {
            /// Tuple element indices are 1-based in the subcolumn syntax.
            NameAndTypePair column{qbit_node->getColumnName() + "." + std::to_string(tuple_idx + 1), qbit->getNestedTupleElementType()};
            new_args.push_back(std::make_shared<ColumnNode>(column, qbit_node->getColumnSource()));
        };

        if (is_strided)
        {
            const size_t num_groups = used_dims / stride;
            /// Group-major order: for each stride group, its first `precision` bit planes (tuple index = group * element_size + bit).
            for (size_t group = 0; group < num_groups; ++group)
                for (size_t bit = 0; bit < precision; ++bit)
                    add_plane(group * element_size + bit);
        }
        else
        {
            for (size_t bit = 0; bit < precision; ++bit)
                add_plane(bit);
        }

        /// Add the trailing constant(s) describing the layout, then the reference vector as the last argument.
        /// Non-strided: a single `dimension` constant. Strided: `stride` followed by `used_dims`.
        ConstantNodePtr last_size_constant;
        if (is_strided)
        {
            new_args.push_back(std::make_shared<ConstantNode>(stride));
            last_size_constant = std::make_shared<ConstantNode>(used_dims);
            new_args.push_back(last_size_constant);
        }
        else
        {
            last_size_constant = std::make_shared<ConstantNode>(dimension);
            new_args.push_back(last_size_constant);
        }

        /// The transposed distance functions propagate nullability from any argument, so the original (user-facing) call may be
        /// Nullable because of the precision, used_dims, or reference-vector arguments. The rewritten internal form drops the precision and
        /// used_dims arguments and casts the reference vector, any of which can otherwise lose the nullability, so force the nullability onto
        /// the trailing size constant whenever the original result was Nullable to keep the rewritten result type identical.
        auto original_result_type = function_node->getResultType();
        if (original_result_type->isNullable() || original_result_type->isLowCardinalityNullable())
            last_size_constant->convertToNullable();

        /// Cast reference vector to match QBit type. This is the only information about the type of the QBit after this pass is applied
        auto expected_ref_vec_type = std::make_shared<DataTypeArray>(qbit->getElementType());

        if (ref_vec_node->getResultType()->equals(*expected_ref_vec_type))
        {
            new_args.push_back(ref_vec_node);
        }
        else
        {
            auto cast_type_constant = std::make_shared<ConstantNode>(expected_ref_vec_type->getName());
            auto cast_function = std::make_shared<FunctionNode>("_CAST");
            cast_function->getArguments().getNodes().push_back(ref_vec_node);
            cast_function->getArguments().getNodes().push_back(cast_type_constant);

            auto cast_function_builder = FunctionFactory::instance().get("_CAST", getContext());
            cast_function->resolveAsFunction(cast_function_builder->build(cast_function->getArgumentColumns()));

            new_args.push_back(cast_function);
        }

        /// Re-resolve function with new arguments
        function_node->getArguments().getNodes() = std::move(new_args);
        auto function_builder = FunctionFactory::instance().get(function_name, getContext());
        function_node->resolveAsFunction(function_builder->build(function_node->getArgumentColumns()));

        if (!function_node->getResultType()->equals(*original_result_type))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{} query tree node does not have a valid source node after running DistanceTransposedPartialReadsPass. Before: {}, "
                "after: {}",
                node->getNodeTypeName(),
                original_result_type->getName(),
                function_node->getResultType()->getName());
    }
};

}

void DistanceTransposedPartialReadsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    DistanceTransposedPartialReadsPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
