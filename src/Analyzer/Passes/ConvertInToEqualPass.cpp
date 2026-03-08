#include <Analyzer/Passes/ConvertInToEqualPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>

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

        /// Strip LowCardinality — it is a storage optimization that does not affect
        /// type compatibility or function semantics, and downstream checks like
        /// WhichDataType and areTypesComparableForEquality cannot see through it.
        auto lhs_type = removeLowCardinality(arguments[0]->getResultType());

        /// IN/NOT IN handle NULLs internally (useDefaultImplementationForNulls = false)
        /// and always return UInt8 (0 or 1). equals/notEquals use default NULL propagation
        /// and return NULL for NULL inputs. When x is Nullable:
        ///   - x NOT IN (v) returns 1 when x is NULL, but x != v returns NULL
        ///   - x IN (v) returns 0 when x is NULL, but x = v returns NULL
        /// So skip conversion when the left-hand side is Nullable.
        if (lhs_type->isNullable())
            return;

        /// IN/NOT IN silently ignore unknown enum values (treat them as non-matching),
        /// but equals/notEquals throw UNKNOWN_ELEMENT_OF_ENUM for values not in the enum definition.
        /// For example, `e NOT IN ('unknown')` returns all rows, but `e != 'unknown'` throws.
        /// Skip conversion for Enum types entirely to preserve correctness.
        WhichDataType lhs_which(lhs_type);
        if (lhs_which.isEnum())
            return;

        /// Create a new constant with Nullable removed from the type.
        /// The IN function wraps the constant type in Nullable during analysis,
        /// but equals/notEquals expect the raw scalar type.
        auto constant_type = constant_node->getResultType();
        auto rhs_type = removeNullable(constant_type);

        /// When the LHS and RHS have the same type, we can convert directly.
        /// When they differ, we convert the constant to the LHS type using
        /// convertFieldToTypeStrict — the same function that IN uses in
        /// SetUtils.cpp to build its Set. This rejects lossy conversions
        /// (e.g. Decimal(9,2) → Decimal(9,1) losing precision, or
        /// Float64 1.5551 → Decimal(9,3) truncating to 1.555).
        ///
        /// This also handles cases like String IN (1): the integer 1 is
        /// converted to the string '1', producing equals(x, '1') which
        /// FunctionComparison can execute. Without this, equals(String, UInt8)
        /// would throw NO_COMMON_TYPE at execution time.
        ///
        /// If the conversion fails or is lossy, IN would never match either,
        /// but we conservatively skip the optimization to avoid changing behavior.
        Field converted_value = value;
        DataTypePtr result_rhs_type = rhs_type;
        if (!lhs_type->equals(*rhs_type))
        {
            auto converted = convertFieldToTypeStrict(value, *rhs_type, *lhs_type);
            if (!converted.has_value())
                return;
            converted_value = std::move(converted.value());
            result_rhs_type = lhs_type;
        }

        const String result_function_name = is_in ? "equals" : "notEquals";

        auto result_function = std::make_shared<FunctionNode>(result_function_name);
        /// Mark as operator for correct AST formatting (e.g. `x = 1` instead of `equals(x, 1)`).
        /// FunctionNode has no isOperator getter, so we always mark it as operator — matching
        /// the convention used in ComparisonTupleEliminationPass, CNF, and other passes.
        result_function->markAsOperator();

        auto new_constant = std::make_shared<ConstantNode>(converted_value, result_rhs_type);
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
