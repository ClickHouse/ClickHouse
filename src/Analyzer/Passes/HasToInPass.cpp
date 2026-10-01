#include <Analyzer/Passes/HasToInPass.h>

#include <Columns/IColumn.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <base/unit.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_has_to_in;
}

namespace
{

class RewriteHasToInVisitor : public InDepthQueryTreeVisitorWithContext<RewriteHasToInVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteHasToInVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_has_to_in])
            return;

        auto * has_function_node = node->as<FunctionNode>();
        if (!has_function_node || (has_function_node->getFunctionName() != "has" && has_function_node->getFunctionName() != "notHas"))
            return;

        auto & has_function_arguments_nodes = has_function_node->getArguments().getNodes();
        if (has_function_arguments_nodes.size() != 2)
            return;

        /// Verify that the first argument is a constant array
        ColumnPtr first_arg_column;
        if (const auto * first_arg_constant = has_function_arguments_nodes[0]->as<ConstantNode>())
            first_arg_column = first_arg_constant->getColumn();
        else
            first_arg_column = tryGetScalarSubqueryColumn(has_function_arguments_nodes[0], getContext());
        if (!first_arg_column)
            return;

        /// The resolver refuses to fold a column this large into a constant, so a secondary server lowers the
        /// rewritten `in` back to `has` and the two servers name the predicate differently.
        if (ConstantValue::wrapToColumnConst(first_arg_column)->byteSize() >= 1_MiB)
            return;

        /// Verify that the first argument is actually an array type
        const auto & first_arg_type = has_function_arguments_nodes[0]->getResultType();
        if (!isArray(first_arg_type))
            return;

        /// The next few checks are to handle differences/quirks between has() and in()
        /// Verify that none of the values in the constant array are NULLs, because has() and in() treat NULLs differently
        const auto & element_type = (typeid_cast<const DataTypeArray *>(first_arg_type.get()))->getNestedType();
        WhichDataType data_type(element_type);
        if (isNullableOrLowCardinalityNullable(element_type) ||
                data_type.isMap() || data_type.isArray() || data_type.isTuple() || data_type.isObject() || data_type.isDynamic() || data_type.isNothing())
            return;

        const Field array_field = (*first_arg_column)[0];
        const auto & array_value = array_field.safeGet<Array>();
        if (array_value.empty())
            return;
        for (const auto & element : array_value)
        {
            if (element.isNull())
                return;
        }

        const auto second_arg_type = has_function_arguments_nodes[1]->getResultType();
        WhichDataType expr_data_type(second_arg_type);
        /// in() takes Nullable and LowCardinality from its needle into its result type; has() always returns UInt8.
        if (isNullableOrLowCardinalityNullable(second_arg_type) ||
                expr_data_type.isMap() || expr_data_type.isArray() || expr_data_type.isTuple() || expr_data_type.isObject() || expr_data_type.isDynamic() || expr_data_type.isVariant() || expr_data_type.isNothing())
            return;

        /// has() on a constant array compares raw Field values (FunctionArrayIndex::executeConst uses
        /// accurateEquals), while in() converts the right-hand side elements to the left-hand side
        /// type via convertFieldToType. The two diverge for types where the Field representation
        /// has different semantic meaning depending on the type (e.g. Date vs DateTime store
        /// different numeric values for the same instant, Enum stores the integer code while String
        /// stores the name, IPv4 has a custom Field encoding distinct from UInt32, etc.).
        ///
        /// Floats are also excluded outright (not just on cross-width): has() uses accurateEquals
        /// where +0.0 == -0.0, while in() builds a Set keyed on raw bits where +0.0 != -0.0. So
        /// `has([0.0], -0.0)` returns 1 but `-0.0 IN (0.0)` returns 0 — even at the same width.
        ///
        /// Only allow the rewrite if the array element type and the needle type are equivalent
        /// under raw Field comparison. The two safe cases are:
        ///   - Exact same non-float type after stripping LowCardinality.
        ///   - Both are native integers (UInt*/Int*), since they all reduce to a single numeric
        ///     Field type and accurateEquals handles mixed widths and signs correctly.
        const auto unwrapped_element_type = removeLowCardinality(element_type);
        const auto unwrapped_second_arg_type = removeLowCardinality(second_arg_type);
        /// `isFloat` covers `BFloat16` in addition to `Float32`/`Float64`; without it,
        /// `has([+0bf16], -0bf16)` would return 1 but the rewritten `in()` returns 0
        /// (the Set is keyed by raw 16-bit payload, so `+0`/`-0` and NaN bit patterns diverge).
        if (isFloat(unwrapped_element_type) || isFloat(unwrapped_second_arg_type))
            return;
        const bool both_native_integers = isNativeInteger(unwrapped_element_type) && isNativeInteger(unwrapped_second_arg_type);
        if (!both_native_integers && !unwrapped_element_type->equals(*unwrapped_second_arg_type))
            return;

        /// Rewrite has(const_array, elem) -> in(elem, const_array), notHas(const_array, elem) -> notIn(elem, const_array)
        /// `transform_null_in` renames the `in` family during resolution, which every pass runs after.
        const auto in_function_name = getInFunctionNameForPassCreatedNode(
            has_function_node->getFunctionName() == "has" ? "in" : "notIn", unwrapped_second_arg_type, getContext());
        if (!in_function_name)
            return;

        if (expr_data_type.isLowCardinality())
            has_function_arguments_nodes[1]
                = foldConstantCast(buildCastFunction(has_function_arguments_nodes[1], unwrapped_second_arg_type, getContext()));

        std::swap(has_function_arguments_nodes[0], has_function_arguments_nodes[1]);
        resolveOrdinaryFunctionNodeByName(*has_function_node, *in_function_name, getContext());
    }
};

}

void RewriteHasToInPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteHasToInVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
