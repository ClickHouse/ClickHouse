#include <memory>
#include <Analyzer/IQueryTreeNode.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/transformQueryForExternalDatabaseAnalyzer.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/convertColumnToType.h>

#include <Analyzer/Utils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/SetUtils.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool external_table_strict_query;
    extern const SettingsBool transform_null_in;
    extern const SettingsBool validate_enum_literals_in_operators;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The value of the size-1 `column` seen through `Nullable` and the active alternative of a `Variant` or `Dynamic`.
/// Empty for NULL.
std::optional<ColumnWithTypeAndName> unwrapCarrier(const ColumnPtr & column, const DataTypePtr & type)
{
    if (column->isNullAt(0))
        return std::nullopt;

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        return unwrapCarrier(assert_cast<const ColumnNullable &>(*column).getNestedColumnPtr(), nullable_type->getNestedType());

    DataTypePtr active_type;
    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
        active_type = variant_type->getVariant(assert_cast<const ColumnVariant &>(*column).globalDiscriminatorAt(0));
    else if (const auto * dynamic_column = typeid_cast<const ColumnDynamic *>(column.get()))
        active_type = dynamic_column->getTypeAt(0);
    else
        return ColumnWithTypeAndName{column, type, ""};

    auto value = convertColumnToTypeOrNull(*column, type, active_type);
    if (!value)
        return std::nullopt;
    return unwrapCarrier(value, active_type);
}

}

bool holdsEnumValue(const ColumnPtr & column, const DataTypePtr & type)
{
    auto value = unwrapCarrier(column, type);
    if (!value)
        return false;

    if (isEnum(value->type))
        return true;

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(value->type.get()))
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*value->column);
        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
            if (holdsEnumValue(tuple_column.getColumnPtr(i), tuple_type->getElement(i)))
                return true;
    }
    return false;
}

namespace
{

bool isStringOrNumber(const DataTypePtr & type)
{
    auto value_type = removeLowCardinalityAndNullable(type);
    return isStringOrFixedString(value_type) || isNumber(value_type);
}

/// Table function arguments may hold identifiers and unresolved functions, which have no type.
bool hasResultType(const QueryTreeNodePtr & node)
{
    if (const auto * function_node = node->as<FunctionNode>())
        return function_node->isResolved();
    return node->as<ColumnNode>() || node->as<ConstantNode>();
}

/// Rewrites each `Enum` leaf of the size-1 constant `column` as a comparison with `operand_type` reads it: the name
/// against a string, the value against a number. Empty if an `Enum` leaf faces another type.
std::optional<ColumnWithTypeAndName> renderEnumLeaves(const ColumnPtr & column, const DataTypePtr & type, const DataTypePtr & operand_type)
{
    if (!holdsEnumValue(column, type))
        return ColumnWithTypeAndName{column, type, ""};

    auto value = unwrapCarrier(column, type);
    auto operand_value_type = removeLowCardinalityAndNullable(operand_type);
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(value->type.get());
    const auto * operand_tuple_type = typeid_cast<const DataTypeTuple *>(operand_value_type.get());
    if (tuple_type && operand_tuple_type && tuple_type->getElements().size() == operand_tuple_type->getElements().size())
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*value->column);
        Columns columns;
        DataTypes types;
        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            auto element = renderEnumLeaves(tuple_column.getColumnPtr(i), tuple_type->getElement(i), operand_tuple_type->getElement(i));
            if (!element)
                return {};
            columns.push_back(element->column);
            types.push_back(element->type);
        }
        DataTypePtr result_type = tuple_type->hasExplicitNames()
            ? std::make_shared<DataTypeTuple>(types, tuple_type->getElementNames())
            : std::make_shared<DataTypeTuple>(types);
        return ColumnWithTypeAndName{ColumnTuple::create(std::move(columns)), result_type, ""};
    }

    if (!isEnum(value->type) || !isStringOrNumber(operand_value_type))
        return {};

    DataTypePtr result_type = isStringOrFixedString(operand_value_type)
        ? DataTypePtr(std::make_shared<DataTypeString>())
        : DataTypePtr(std::make_shared<DataTypeInt64>());
    auto result = convertColumnToTypeOrNull(*value->column, value->type, result_type);
    if (!result)
        return {};
    return ColumnWithTypeAndName{result, result_type, ""};
}

bool holdsEnumConstant(const QueryTreeNodePtr & node)
{
    if (const auto * constant_node = node->as<ConstantNode>())
        if (holdsEnumValue(constant_node->getColumn()->getDataColumnPtr(), constant_node->getResultType()))
            return true;

    for (const auto & child : node->getChildren())
        if (child && holdsEnumConstant(child))
            return true;
    return false;
}

/// A constant that still holds an `Enum` value would reach the external database as its number; a conjunct with one is applied locally only.
void removeConjunctsHoldingEnumConstants(QueryTreeNodePtr & filter, const ContextPtr & context)
{
    auto throw_if_strict = [&]
    {
        if (context->getSettingsRef()[Setting::external_table_strict_query])
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Query contains expressions that cannot be pushed down (and external_table_strict_query=true)");
    };

    auto * function = filter->as<FunctionNode>();
    if (!function || function->getFunctionName() != "and")
    {
        if (holdsEnumConstant(filter))
        {
            throw_if_strict();
            filter = {};
        }
        return;
    }

    auto & conjuncts = function->getArguments().getNodes();
    if (std::erase_if(conjuncts, holdsEnumConstant) == 0)
        return;
    throw_if_strict();

    if (conjuncts.empty())
    {
        filter = {};
    }
    else if (conjuncts.size() == 1)
    {
        QueryTreeNodePtr remaining = conjuncts.front();
        filter = std::move(remaining);
    }
    else
    {
        const auto function_impl = FunctionFactory::instance().get("and", context);
        function->resolveAsFunction(function_impl->build(function->getArgumentColumns()));
    }
}

class PrepareForExternalDatabaseVisitor : public InDepthQueryTreeVisitor<PrepareForExternalDatabaseVisitor>
{
public:
    explicit PrepareForExternalDatabaseVisitor(GetSetElementParams set_params_)
        : set_params(set_params_)
    {}

    void visitImpl(QueryTreeNodePtr & node) const
    {
        if (auto * function_node = node->as<FunctionNode>())
        {
            renderEnumConstants(*function_node);
            return;
        }

        auto * constant_node = node->as<ConstantNode>();
        if (constant_node)
        {
            auto result_type = constant_node->getResultType();
            if (isDate(result_type) || isDateTime(result_type) || isDateTime64(result_type))
            {
                /// Use string representation of constant date and time values
                /// The code is ugly - how to convert artbitrary Field to proper string representation?
                /// (maybe we can just consider numbers as unix timestamps?)
                auto result_column = result_type->createColumnConst(1, constant_node->getValue());
                const IColumn & inner_column = result_column->getDataColumn();

                WriteBufferFromOwnString out;
                result_type->getDefaultSerialization()->serializeText(inner_column, 0, out, FormatSettings());
                node = std::make_shared<ConstantNode>(out.str(), std::move(result_type));
            }
        }
    }

private:
    /// A constant that holds an `Enum` value is written in the domain its operand is compared in.
    void renderEnumConstants(FunctionNode & function_node) const
    {
        auto & arguments = function_node.getArguments().getNodes();
        if (arguments.size() != 2 || !std::ranges::all_of(arguments, hasResultType))
            return;

        const auto & name = function_node.getFunctionName();
        if (name == "in" || name == "notIn")
        {
            if (auto set = renderInSet(arguments[0], arguments[1]))
                arguments[1] = std::move(set);
        }
        else if (name == "equals" || name == "notEquals" || name == "less" || name == "greater"
            || name == "lessOrEquals" || name == "greaterOrEquals")
        {
            for (size_t i = 0; i < 2; ++i)
                if (auto constant = renderComparisonConstant(arguments[i], arguments[1 - i]))
                    arguments[i] = std::move(constant);
        }
    }

    static QueryTreeNodePtr renderComparisonConstant(const QueryTreeNodePtr & argument, const QueryTreeNodePtr & operand)
    {
        const auto * constant_node = argument->as<ConstantNode>();
        if (!constant_node || operand->as<ConstantNode>())
            return nullptr;

        const auto & column = constant_node->getColumn()->getDataColumnPtr();
        auto rendered = renderEnumLeaves(column, constant_node->getResultType(), operand->getResultType());
        if (!rendered || rendered->column == column)
            return nullptr;
        return std::make_shared<ConstantNode>(ColumnConst::create(rendered->column, 1), rendered->type);
    }

    /// The members of the set the local query builds for `lhs IN rhs`, as a scalar, a row or a list of them.
    QueryTreeNodePtr renderInSet(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs) const
    {
        const auto * constant_node = rhs->as<ConstantNode>();
        if (!constant_node || !holdsEnumValue(constant_node->getColumn()->getDataColumnPtr(), constant_node->getResultType()))
            return nullptr;

        /// Keys as `getSetElementsForConstantValue` sees them: a one-element tuple stays packed. A `FixedString` key gets
        /// the unpadded name.
        auto lhs_type = lhs->getResultType();
        const auto * lhs_tuple_type = typeid_cast<const DataTypeTuple *>(lhs_type.get());
        bool packed_key = lhs_tuple_type && lhs_tuple_type->getElements().size() == 1;
        DataTypes key_types = lhs_tuple_type && !lhs_tuple_type->getElements().empty() ? lhs_tuple_type->getElements() : DataTypes{lhs_type};
        for (auto & key_type : key_types)
        {
            if (!isStringOrNumber(key_type))
                return nullptr;
            if (isFixedString(removeLowCardinalityAndNullable(key_type)))
            {
                DataTypePtr string_type = std::make_shared<DataTypeString>();
                key_type = removeLowCardinality(key_type)->isNullable() ? makeNullable(string_type) : string_type;
            }
        }
        DataTypePtr set_lhs_type = lhs_tuple_type ? std::make_shared<DataTypeTuple>(key_types) : key_types.front();

        auto set = getSetElementsForConstantValue(set_lhs_type, constant_node->getColumn(), constant_node->getResultType(), set_params);
        if (set.empty() || set.front().column->empty())
            return nullptr;

        Tuple members;
        for (size_t row = 0; row < set.front().column->size(); ++row)
        {
            if (set.size() == 1)
            {
                auto member = (*set.front().column)[row];
                members.push_back(packed_key ? member.safeGet<Tuple>()[0] : std::move(member));
                continue;
            }
            Tuple key;
            for (const auto & key_column : set)
                key.push_back((*key_column.column)[row]);
            members.push_back(std::move(key));
        }

        if (members.size() == 1)
            return std::make_shared<ConstantNode>(std::move(members.front()));
        return std::make_shared<ConstantNode>(Field(std::move(members)));
    }

    GetSetElementParams set_params;
};

}

ASTPtr getASTForExternalDatabaseFromQueryTree(ContextPtr context, const QueryTreeNodePtr & query_tree, const TableExpressionNodePtr & table_expression)
{
    auto replacement_table_expression = table_expression->clone();
    auto new_tree = query_tree->cloneAndReplace(table_expression, static_pointer_cast<ITableExpressionNode>(replacement_table_expression));

    const auto & settings = context->getSettingsRef();
    PrepareForExternalDatabaseVisitor visitor(GetSetElementParams{
        .transform_null_in = settings[Setting::transform_null_in],
        .forbid_unknown_enum_values = settings[Setting::validate_enum_literals_in_operators],
    });
    visitor.visit(new_tree);
    auto * query_node = new_tree->as<QueryNode>();

    const auto & join_tree = query_node->getJoinTreeNode();
    bool allow_where = true;
    if (const auto * join_node = join_tree->as<JoinNode>())
    {
        if (join_node->getKind() == JoinKind::Left)
            allow_where = join_node->getLeftTableExpressionNode()->isEqual(*replacement_table_expression);
        else if (join_node->getKind() == JoinKind::Right)
            allow_where = join_node->getRightTableExpressionNode()->isEqual(*replacement_table_expression);
        else
            allow_where = (join_node->getKind() == JoinKind::Inner);
    }

    /// Remove all sub-expressions (operands of AND) that depend on columns from other tables.
    /// This is needed for a correct push-down of these filters to an external storage.
    if (allow_where)
    {
        if (query_node->hasPrewhere())
            removeExpressionsThatDoNotDependOnTableIdentifiers(query_node->getPrewhere(), replacement_table_expression, context);
        if (query_node->hasPrewhere())
            removeConjunctsHoldingEnumConstants(query_node->getPrewhere(), context);
        if (query_node->hasWhere())
            removeExpressionsThatDoNotDependOnTableIdentifiers(query_node->getWhere(), replacement_table_expression, context);
        if (query_node->hasWhere())
            removeConjunctsHoldingEnumConstants(query_node->getWhere(), context);
    }

    /// The external database parses this text itself, so a date-time constant must stay in its text form.
    auto query_node_ast = query_node->toAST({ .add_cast_for_constants = false,
                                              .date_time_constants_as_numbers = false,
                                              .fully_qualified_identifiers = false });
    const IAST * ast = query_node_ast.get();

    if (const auto * ast_subquery = ast->as<ASTSubquery>())
        ast = ast_subquery->children.at(0).get();

    const auto * union_ast = ast->as<ASTSelectWithUnionQuery>();
    if (!union_ast)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "QueryNode AST ({}) is not a ASTSelectWithUnionQuery", query_node_ast->getID());

    if (union_ast->list_of_selects->children.size() != 1)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "QueryNode AST is not a single ASTSelectQuery, got {}", union_ast->list_of_selects->children.size());

    ASTPtr select_query = union_ast->list_of_selects->children.at(0);
    auto * select_query_typed = select_query->as<ASTSelectQuery>();
    if (!select_query_typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ASTSelectQuery, got {}", select_query ? select_query->formatForErrorMessage() : "nullptr");
    if (!allow_where)
    {
        /// Nothing is pushed down from this side of the join, so neither filter may reach the external
        /// database. `PREWHERE` has to go as well: the external table engines do not support it, so a
        /// surviving `PREWHERE` can only belong to the other, joined table and must not be presented to
        /// the caller as a filter on this one (`rejectOuterFilterForQueryBackedExternalSourceIfStrict`
        /// would otherwise reject it).
        select_query_typed->setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
        select_query_typed->setExpression(ASTSelectQuery::Expression::PREWHERE, nullptr);
    }
    return select_query;
}

}
