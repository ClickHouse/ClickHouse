#include <Analyzer/Passes/IfTransformStringsToEnumPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/ConstantValue.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Columns/validateColumnType.h>

#include <Common/typeid_cast.h>

#include <base/unit.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>

#include <Functions/FunctionFactory.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_if_transform_strings_to_enum;
}

namespace
{

/// We place strings in ascending order here under the assumption it could speed up String to Enum conversion.
template <typename EnumType>
auto getDataEnumType(const std::set<std::string> & string_values)
{
    using EnumValues = typename EnumType::Values;
    EnumValues enum_values;
    enum_values.reserve(string_values.size());

    size_t number = 1;
    for (const auto & value : string_values)
        enum_values.emplace_back(value, number++);

    return std::make_shared<EnumType>(std::move(enum_values));
}

DataTypePtr getEnumType(const std::set<std::string> & string_values)
{
    if (string_values.size() >= 255)
        return getDataEnumType<DataTypeEnum16>(string_values);
    return getDataEnumType<DataTypeEnum8>(string_values);
}

/// `createCastFunction` builds a resolved `_CAST(<string literal>, Enum...)` function node but, unlike
/// normal function resolution, does not constant-fold it. When the rewritten query is shipped to a
/// remote shard / parallel replica, that shard re-analyzes the AST and folds `_CAST('a', 'Enum8...')`
/// into a `ConstantNode` holding the Enum value. The action-node naming on the initiator and the shard
/// then diverges (`_CAST('a'_String, ...)` vs `_CAST(1_Enum8(...), ...)`), so the converting actions on
/// the initiator cannot find the column the shard produced -> THERE_IS_NO_COLUMN (issue #74716). Fold the
/// cast here, exactly as `resolveFunction` does, so both sides name the constant identically. The original
/// `_CAST` is kept as the constant's source expression, matching a shard-folded constant.
QueryTreeNodePtr foldConstantCast(const QueryTreeNodePtr & cast_node)
{
    const auto * cast_function = cast_node->as<FunctionNode>();
    if (!cast_function || !cast_function->isResolved())
        return cast_node;

    auto function_base = cast_function->getFunction();
    if (!function_base || !function_base->isSuitableForConstantFolding())
        return cast_node;

    auto argument_columns = cast_function->getArgumentColumns();
    if (!std::all_of(argument_columns.begin(), argument_columns.end(), [](const auto & arg) { return arg.column && isColumnConst(*arg.column); }))
        return cast_node;

    auto result_type = function_base->getResultType();
    auto executable_function = function_base->prepare(argument_columns);
    auto column = executable_function->execute(argument_columns, result_type, 1, /* dry_run = */ true);
    if (column && column->empty() && isColumnConst(*column))
        column = column->cloneResized(1);

    const auto * column_const = column ? typeid_cast<const ColumnConst *>(column.get()) : nullptr;
    if (!column_const || column_const->getDataColumn().isDummy())
        return cast_node;

    /// Sanity check mirrored from resolveFunction.
    if (!columnMatchesType(*column, *result_type))
        return cast_node;

    /// Match resolveFunction's `byteSize() < 1_MiB` guard. A large folded value (e.g. a `transform`
    /// Enum-array map >= 1 MiB) is left as a `_CAST` function by the shard, so the initiator must not
    /// fold it either, otherwise the action-node names diverge again and #74716 reappears.
    if (column->byteSize() >= 1_MiB)
        return cast_node;

    /// Mirror resolveFunction's determinism propagation: a value folded from a non-deterministic
    /// source (e.g. an `if` branch that is `currentUser()`) must stay non-deterministic, otherwise
    /// downstream hasNonDeterministic()/assertDeterministic() see a different contract than normal folding.
    bool all_arguments_are_deterministic = true;
    for (const auto & argument : cast_function->getArguments().getNodes())
    {
        if (const auto * argument_constant = argument->as<ConstantNode>())
            all_arguments_are_deterministic &= argument_constant->isDeterministic();
    }
    const bool is_deterministic = all_arguments_are_deterministic && function_base->isDeterministic();

    return std::make_shared<ConstantNode>(
        ConstantValue{column_const->getPtr(), std::move(result_type)}, cast_node, is_deterministic);
}

/// if(arg1, arg2, arg3) will be transformed to if(arg1, _CAST(arg2, Enum...), _CAST(arg3, Enum...))
/// where Enum is generated based on the possible values stored in string_values
void changeIfArguments(
    FunctionNode & if_node, const std::set<std::string> & string_values, const ContextPtr & context)
{
    auto result_type = getEnumType(string_values);

    auto & argument_nodes = if_node.getArguments().getNodes();

    argument_nodes[1] = foldConstantCast(createCastFunction(argument_nodes[1], result_type, context));
    argument_nodes[2] = foldConstantCast(createCastFunction(argument_nodes[2], result_type, context));

    auto if_resolver = FunctionFactory::instance().get("if", context);

    if_node.resolveAsFunction(if_resolver->build(if_node.getArgumentColumns()));
}

/// transform(value, array_from, array_to, default_value) will be transformed to transform(value, array_from, _CAST(array_to, Array(Enum...)), _CAST(default_value, Enum...))
/// where Enum is generated based on the possible values stored in string_values
void changeTransformArguments(
    FunctionNode & transform_node,
    const std::set<std::string> & string_values,
    const ContextPtr & context)
{
    auto result_type = getEnumType(string_values);

    auto & arguments = transform_node.getArguments().getNodes();

    auto & array_to = arguments[2];
    auto & default_value = arguments[3];

    array_to = foldConstantCast(createCastFunction(array_to, std::make_shared<DataTypeArray>(result_type), context));
    default_value = foldConstantCast(createCastFunction(default_value, std::move(result_type), context));

    auto transform_resolver = FunctionFactory::instance().get("transform", context);

    transform_node.resolveAsFunction(transform_resolver->build(transform_node.getArgumentColumns()));
}

void wrapIntoToString(FunctionNode & function_node, QueryTreeNodePtr arg, ContextPtr context)
{
    auto to_string_function = FunctionFactory::instance().get("toString", std::move(context));
    QueryTreeNodes arguments{ std::move(arg) };
    function_node.getArguments().getNodes() = std::move(arguments);

    function_node.resolveAsFunction(to_string_function->build(function_node.getArgumentColumns()));

    chassert(isString(removeNullable(function_node.getResultType())));
}

class ConvertStringsToEnumVisitor : public InDepthQueryTreeVisitorWithContext<ConvertStringsToEnumVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertStringsToEnumVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_if_transform_strings_to_enum])
            return;

        auto * function_node = node->as<FunctionNode>();

        if (!function_node)
            return;

        const auto & context = getContext();

        /// to preserve return type (String) of the current function_node, we wrap the newly
        /// generated function nodes into toString

        std::string_view function_name = function_node->getFunctionName();
        if (function_name == "if")
        {
            if (function_node->getArguments().getNodes().size() != 3)
                return;

            auto modified_if_node = function_node->clone();
            auto * function_if_node = modified_if_node->as<FunctionNode>();
            auto & argument_nodes = function_if_node->getArguments().getNodes();

            const auto * first_literal = argument_nodes[1]->as<ConstantNode>();
            const auto * second_literal = argument_nodes[2]->as<ConstantNode>();

            if (!first_literal || !second_literal)
                return;

            if (!isString(first_literal->getResultType()) || !isString(second_literal->getResultType()))
                return;

            std::set<std::string> string_values;
            string_values.insert(first_literal->getValue().safeGet<std::string>());
            string_values.insert(second_literal->getValue().safeGet<std::string>());

            changeIfArguments(*function_if_node, string_values, context);
            wrapIntoToString(*function_node, std::move(modified_if_node), context);
            return;
        }

        if (function_name == "transform")
        {
            if (function_node->getArguments().getNodes().size() != 4)
                return;

            auto modified_transform_node = function_node->clone();
            auto * function_modified_transform_node = modified_transform_node->as<FunctionNode>();
            auto & argument_nodes = function_modified_transform_node->getArguments().getNodes();

            if (!isString(removeNullable(function_node->getResultType())))
                return;

            const auto * literal_to = argument_nodes[2]->as<ConstantNode>();
            const auto * literal_default = argument_nodes[3]->as<ConstantNode>();

            if (!literal_to || !literal_default)
                return;

            if (!isArray(literal_to->getResultType()) || !isString(literal_default->getResultType()))
                return;

            auto array_to = literal_to->getValue().safeGet<Array>();

            if (array_to.empty())
                return;

            if (!std::all_of(
                    array_to.begin(),
                    array_to.end(),
                    [](const auto & field) { return field.getType() == Field::Types::Which::String; }))
                return;

            /// collect possible string values
            std::set<std::string> string_values;

            for (const auto & value : array_to)
                string_values.insert(value.safeGet<std::string>());

            string_values.insert(literal_default->getValue().safeGet<std::string>());

            changeTransformArguments(*function_modified_transform_node, string_values, context);
            wrapIntoToString(*function_node, std::move(modified_transform_node), context);
            return;
        }
    }
};

}

void IfTransformStringsToEnumPass::run(QueryTreeNodePtr & query, ContextPtr context)
{
    ConvertStringsToEnumVisitor visitor(std::move(context));
    visitor.visit(query);
}

}
