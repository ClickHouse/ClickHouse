#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/DistanceTransposedPartialReadsPass.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
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

        /// Quantized variants dequantize a QBit(Int8) on the fly and take a full-precision Float32 reference vector.
        const bool is_quantized
            = (function_name == "L2DistanceTransposedQuantized" || function_name == "cosineDistanceTransposedQuantized"
               || function_name == "dotProductTransposedQuantized");

        if (!is_distance_function && !is_quantized)
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

        /// A Variant or Dynamic reference vector carries per-row type and null semantics: the distance function is
        /// evaluated separately for each stored type and NULL rows must propagate as NULL. The optimization casts the
        /// reference vector to a plain Array(element_type), which cannot preserve those semantics, so it must not be
        /// applied to such a reference vector - leave the call for the unoptimized distance function to handle.
        /// A Variant is the more dangerous case: casting it to Array throws (`CAST AS Array can only be performed
        /// between same-dimensional Array, Map or String types`) while the rewrite is being built below, so without
        /// this guard the whole query would fail instead of running. (A Dynamic reference vector would instead be
        /// caught by the result-type guard at the end of the pass, because its result type differs from the rewritten
        /// Nullable(Float64); rejecting both here is simpler and keeps the two special types together.)
        const auto ref_vec_type = ref_vec_node->getResultType();
        if (isVariant(ref_vec_type) || isDynamic(ref_vec_type))
            return;

        /// A Nullable reference vector must be left unoptimized as well. The rewrite casts the reference vector to a plain
        /// Array(element_type) with an internal (non keep_nullable) _CAST, and _CAST of a NULL to a non-Nullable type throws
        /// `Cannot convert NULL to a non-nullable type` while the rewrite is being built. In practice the only Nullable
        /// reference vector that can occur is a bare NULL constant (type Nullable(Nothing)) - Nullable(Array(...)) is not a
        /// constructible type in ClickHouse - and that constant-NULL case is also caught by the "result type is only NULL"
        /// guard further down. Rejecting any Nullable reference vector here, next to the Variant/Dynamic guard, keeps the
        /// "special reference vectors are not optimized" contract explicit and robust. The unoptimized distance function
        /// returns NULL for such a reference through its default Nullable handling. This is independent of the QBit column's
        /// own nullability: the column's null map still flows through the Nullable bit-plane subcolumns when the optimization
        /// does apply.
        if (ref_vec_type->isNullable())
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

        /// Apply the optimization. The QBit column may be Nullable: we read its bit-plane subcolumns as Nullable(FixedString)
        /// (they carry the same per-row null map), so the null map propagates to the result through the distance function's
        /// default Nullable handling, exactly as it would for the unoptimised full-column read.
        const auto column_type = qbit_node->getColumnType();
        const bool is_nullable = column_type->isNullable();
        const auto * qbit = checkAndGetDataType<DataTypeQBit>(removeNullable(column_type).get());

        if (!qbit)
            return;

        /// The quantized variants only operate on QBit(Int8) codes. If the type does not match, leave the function untouched:
        /// the function's own getReturnTypeImpl will produce the user-facing error.
        if (is_quantized && !WhichDataType(qbit->getElementType()).isInt8())
            return;

        /// The Nullable(QBit) rewrite (reading the bit planes as Nullable(FixedString) so the column's per-row null map flows
        /// through the distance function's default Nullable handling) is only exercised for the non-quantized transposed
        /// functions in this change. The quantized variants dequantize the Int8 codes and handle the reference vector
        /// differently (a Float reference is cast to Array(Float32), an Array(Int8) reference is dequantized like the QBit),
        /// so their Nullable(QBit(Int8)) path needs its own focused coverage before it is optimized. Until that exists, leave
        /// a Nullable quantized QBit column for the unoptimized function to handle. This only declines an optimization - the
        /// unoptimized distance function is the ground truth and returns the same result - so it cannot change any query result.
        if (is_quantized && is_nullable)
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
            /// Tuple element indices are 1-based in the subcolumn syntax. When the QBit column is Nullable, the bit-plane
            /// subcolumn is Nullable(FixedString) too, which lets the per-row null map reach the distance function.
            auto plane_type = qbit->getNestedTupleElementType();
            if (is_nullable)
                plane_type = makeNullable(plane_type);
            NameAndTypePair column{qbit_node->getColumnName() + "." + std::to_string(tuple_idx + 1), plane_type};
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
        /// When the QBit column itself is Nullable, the nullability already flows through the Nullable bit-plane subcolumns, so the size
        /// constant does not need to carry it.
        auto original_result_type = function_node->getResultType();
        if (!is_nullable && (original_result_type->isNullable() || original_result_type->isLowCardinalityNullable()))
            last_size_constant->convertToNullable();

        /// Cast reference vector to match QBit type. For the non-quantized functions this is the only information about the type of the
        /// QBit after this pass is applied. The quantized functions dequantize the Int8 codes to Float32 levels on the fly, so a Float
        /// reference (the full-precision query) must be cast to Array(Float32); a quantized Array(Int8) reference is left unchanged and
        /// dequantized on the fly exactly like the QBit codes.
        DataTypePtr expected_ref_vec_type;
        if (is_quantized)
        {
            const auto * ref_array = checkAndGetDataType<DataTypeArray>(ref_vec_type.get());
            if (ref_array && WhichDataType(ref_array->getNestedType()).isInt8())
                expected_ref_vec_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>());
            else
                expected_ref_vec_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
        }
        else
            expected_ref_vec_type = std::make_shared<DataTypeArray>(qbit->getElementType());

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

            /// If the reference vector is a constant (a literal or a `WITH`-bound constant), fold the `_CAST` here,
            /// exactly as `resolveFunction` would. Otherwise the initiator keeps a live `_CAST(<constant>, <type>)`
            /// named after its source expression, while a remote replica receives the constant serialized as a
            /// plain literal (`FunctionNode::toASTImpl` drops the source expression for `_CAST`), folds it, and
            /// names it by value -- so the initiator cannot find the column in blocks received from the replica
            /// (NOT_FOUND_COLUMN_IN_BLOCK, issue #110719). A non-constant reference (e.g. `__getScalar(...)` from a
            /// scalar subquery) is not folded and is handled by `ConstantNode::receivedFromInitiatorServer`.
            new_args.push_back(foldConstantCast(cast_function));
        }

        /// Re-resolve function with the rewritten arguments.
        function_node->getArguments().getNodes() = std::move(new_args);
        auto function_builder = FunctionFactory::instance().get(function_name, getContext());
        function_node->resolveAsFunction(function_builder->build(function_node->getArgumentColumns()));

        /// The rewrite must preserve the result type of the call. The distance function always returns Float64, wrapped
        /// in Nullable exactly when one of its arguments is Nullable; the rewrite carries that nullability onto the
        /// trailing size constant (or through the Nullable bit-plane subcolumns when the QBit column itself is Nullable)
        /// so the rewritten result type matches the original. The only other type-changing reference vectors - Variant
        /// and Dynamic - are excluded up front. Reaching a mismatch here therefore means the optimization silently
        /// changed the query result type, which is a bug in this pass rather than a user error, so fail loudly instead
        /// of degrading to an unoptimized result.
        if (!function_node->getResultType()->equals(*original_result_type))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "DistanceTransposedPartialReadsPass changed the result type of function {} from {} to {}",
                function_name,
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
