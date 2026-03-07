#include <Analyzer/Passes/ConvertInToEqualPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Utils.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_in_to_equal;
}

namespace
{

class ConvertInToEqualPassVisitor : public InDepthQueryTreeVisitorWithContext<ConvertInToEqualPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertInToEqualPassVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_in_to_equal])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const auto & function_name = function_node->getFunctionName();

        bool is_in = (function_name == "in" || function_name == "globalIn");
        bool is_not_in = (function_name == "notIn" || function_name == "globalNotIn");
        if (!is_in && !is_not_in)
            return;

        auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 2)
            return;

        /// The right-hand side must be a constant
        auto * constant_node = arguments[1]->as<ConstantNode>();
        if (!constant_node)
            return;

        const auto & value = constant_node->getValue();

        /// Only convert scalar constants. Tuple, Array, Map, Object are compound types
        /// that IN handles differently (e.g. Tuple = multiple values, Array = set of values).
        const auto field_type = value.getType();
        if (field_type == Field::Types::Which::Tuple
            || field_type == Field::Types::Which::Array
            || field_type == Field::Types::Which::Map
            || field_type == Field::Types::Which::Object)
            return;

        /// x IN NULL is not equivalent to x = NULL (IN returns 0, = returns NULL)
        if (value.isNull())
            return;

        /// IN/NOT IN handle NULLs internally (useDefaultImplementationForNulls = false)
        /// and always return UInt8 (0 or 1). equals/notEquals use default NULL propagation
        /// and return NULL for NULL inputs. When x is Nullable:
        ///   - x NOT IN (v) returns 1 when x is NULL, but x != v returns NULL
        ///   - x IN (v) returns 0 when x is NULL, but x = v returns NULL
        /// So skip conversion when the left-hand side is Nullable.
        auto lhs_type = arguments[0]->getResultType();
        if (isNullableOrLowCardinalityNullable(lhs_type))
            return;

        /// IN/NOT IN silently ignore unknown enum values (treat them as non-matching),
        /// but equals/notEquals throw UNKNOWN_ELEMENT_OF_ENUM for values not in the enum definition.
        /// For example, `e NOT IN ('unknown')` returns all rows, but `e != 'unknown'` throws.
        /// Skip conversion for Enum types entirely to preserve correctness.
        /// Unwrap LowCardinality to catch LowCardinality(Enum(...)) as well.
        WhichDataType lhs_which(removeLowCardinality(lhs_type));
        if (lhs_which.isEnum())
            return;

        const String result_function_name = is_in ? "equals" : "notEquals";

        auto result_function = std::make_shared<FunctionNode>(result_function_name);
        /// Mark as operator for correct AST formatting (e.g. `x = 1` instead of `equals(x, 1)`).
        /// FunctionNode has no isOperator getter, so we always mark it as operator — matching
        /// the convention used in ComparisonTupleEliminationPass, CNF, and other passes.
        result_function->markAsOperator();

        /// Create a new constant with Nullable removed from the type.
        /// The IN function wraps the constant type in Nullable during analysis,
        /// but equals/notEquals expect the raw scalar type.
        auto constant_type = constant_node->getResultType();
        auto rhs_type = removeNullable(constant_type);

        /// Check type compatibility before attempting to resolve equals/notEquals.
        /// IN accepts nearly any type combination, but equals has stricter rules
        /// (e.g. Date vs UInt is rejected). Skip conversion if the types are incompatible.
        if (!areTypesComparableForEquality(lhs_type, rhs_type))
            return;

        auto new_constant = std::make_shared<ConstantNode>(value, rhs_type);
        if (constant_node->hasSourceExpression())
            new_constant->getSourceExpression() = constant_node->getSourceExpression();

        result_function->getArguments().getNodes() = {arguments[0], std::move(new_constant)};

        resolveOrdinaryFunctionNodeByName(*result_function, result_function_name, getContext());

        node = std::move(result_function);
    }
};

}

void ConvertInToEqualPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    ConvertInToEqualPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
