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
#include <Interpreters/convertColumnToType.h>

#include <Analyzer/Utils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/SetUtils.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool transform_null_in;
    extern const SettingsBool validate_enum_literals_in_operators;
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Whether the value at `row` is an `Enum` value, also as the non-NULL value of a `Nullable`, the active alternative of
/// a `Variant` or `Dynamic`, or (with `search_tuples`) inside a `Tuple`. `Array` and `Map` are not searched.
bool holdsEnumValue(const IColumn & column, size_t row, const DataTypePtr & type, bool search_tuples)
{
    if (isEnum(type))
        return true;

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto & nullable_column = assert_cast<const ColumnNullable &>(column);
        return !nullable_column.isNullAt(row)
            && holdsEnumValue(nullable_column.getNestedColumn(), row, nullable_type->getNestedType(), search_tuples);
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()); tuple_type && search_tuples)
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(column);
        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
            if (holdsEnumValue(tuple_column.getColumn(i), row, tuple_type->getElement(i), search_tuples))
                return true;
        return false;
    }

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        const auto & variant_column = assert_cast<const ColumnVariant &>(column);
        auto discriminator = variant_column.globalDiscriminatorAt(row);
        return discriminator != ColumnVariant::NULL_DISCRIMINATOR
            && holdsEnumValue(variant_column.getVariantByGlobalDiscriminator(discriminator), variant_column.offsetAt(row),
                              variant_type->getVariant(discriminator), search_tuples);
    }

    if (const auto * dynamic_column = typeid_cast<const ColumnDynamic *>(&column))
    {
        auto active_type = dynamic_column->getTypeAt(row);
        return active_type && isEnum(active_type);
    }

    return false;
}

bool isStringOrNativeNumber(const DataTypePtr & type)
{
    auto value_type = removeLowCardinalityAndNullable(type);
    return isStringOrFixedString(value_type) || isNativeNumber(value_type);
}

/// Rewrites each `Enum` leaf of the size-1 constant `column` the way a comparison with `operand_type` reads it: as
/// its name against a string, as its value against a number. Other leaves are kept. Empty if an `Enum` leaf faces
/// another type.
std::optional<ColumnWithTypeAndName> renderEnumLeaves(const ColumnPtr & column, const DataTypePtr & type, const DataTypePtr & operand_type)
{
    if (!holdsEnumValue(*column, 0, type, /*search_tuples=*/ true))
        return ColumnWithTypeAndName{column, type, ""};

    auto operand_value_type = removeLowCardinalityAndNullable(operand_type);
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
    const auto * operand_tuple_type = typeid_cast<const DataTypeTuple *>(operand_value_type.get());
    if (tuple_type && operand_tuple_type && tuple_type->getElements().size() == operand_tuple_type->getElements().size())
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*column);
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

    if (!holdsEnumValue(*column, 0, type, /*search_tuples=*/ false) || !isStringOrNativeNumber(operand_value_type))
        return {};

    ColumnPtr value = column;
    DataTypePtr value_type = type;
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        value = assert_cast<const ColumnNullable &>(*column).getNestedColumnPtr();
        value_type = nullable_type->getNestedType();
    }

    DataTypePtr result_type = isStringOrFixedString(operand_value_type)
        ? DataTypePtr(std::make_shared<DataTypeString>())
        : DataTypePtr(std::make_shared<DataTypeInt64>());
    auto result = convertColumnToTypeOrNull(*value, value_type, result_type);
    if (!result)
        return {};
    return ColumnWithTypeAndName{result, result_type, ""};
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
        if (arguments.size() != 2)
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
        if (!holdsEnumValue(*column, 0, constant_node->getResultType(), /*search_tuples=*/ true))
            return nullptr;

        auto rendered = renderEnumLeaves(column, constant_node->getResultType(), operand->getResultType());
        if (!rendered)
            return nullptr;
        return std::make_shared<ConstantNode>(ColumnConst::create(rendered->column, 1), rendered->type);
    }

    /// The members of the set the local query builds for `lhs IN rhs`, as a scalar, a row or a list of them.
    QueryTreeNodePtr renderInSet(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs) const
    {
        const auto * constant_node = rhs->as<ConstantNode>();
        if (!constant_node || !holdsEnumValue(*constant_node->getColumn()->getDataColumnPtr(), 0, constant_node->getResultType(), /*search_tuples=*/ true))
            return nullptr;

        /// Keys unpacked as `getSetElementsForConstantValue` does. A `FixedString` key gets the unpadded name.
        auto lhs_type = lhs->getResultType();
        const auto * lhs_tuple_type = typeid_cast<const DataTypeTuple *>(lhs_type.get());
        DataTypes key_types = lhs_tuple_type && lhs_tuple_type->getElements().size() > 1 ? lhs_tuple_type->getElements() : DataTypes{lhs_type};
        for (auto & key_type : key_types)
        {
            if (!isStringOrNativeNumber(key_type))
                return nullptr;
            if (isFixedString(removeLowCardinalityAndNullable(key_type)))
            {
                DataTypePtr string_type = std::make_shared<DataTypeString>();
                key_type = removeLowCardinality(key_type)->isNullable() ? makeNullable(string_type) : string_type;
            }
        }
        DataTypePtr set_lhs_type = key_types.size() > 1 ? std::make_shared<DataTypeTuple>(key_types) : key_types.front();

        auto set = getSetElementsForConstantValue(set_lhs_type, constant_node->getColumn(), constant_node->getResultType(), set_params);
        if (set.empty() || set.front().column->empty())
            return nullptr;

        Tuple members;
        for (size_t row = 0; row < set.front().column->size(); ++row)
        {
            if (set.size() == 1)
            {
                members.push_back((*set.front().column)[row]);
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
        if (query_node->hasWhere())
            removeExpressionsThatDoNotDependOnTableIdentifiers(query_node->getWhere(), replacement_table_expression, context);
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
