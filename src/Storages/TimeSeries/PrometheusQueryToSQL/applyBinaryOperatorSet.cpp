#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorSet.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForBinaryOperator.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

void checkArgumentTypesForSetBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    const SQLQueryPiece & left_argument,
    const SQLQueryPiece & right_argument,
    const ConverterContext & context)
{
    if (left_argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "{} expects two arguments of type {}, but expression {} has type {}",
                        operator_node->getTypeAndName(), ResultType::INSTANT_VECTOR,
                        getPromQLText(left_argument, context), left_argument.type);
    }

    if (right_argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "{} expects two arguments of type {}, but expression {} has type {}",
                        operator_node->getTypeAndName(), ResultType::INSTANT_VECTOR,
                        getPromQLText(right_argument, context), right_argument.type);
    }

    if (operator_node->group_left)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "{} doesn't allow group_left",
                        operator_node->getTypeAndName());
    }

    if (operator_node->group_right)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "{} doesn't allow group_right",
                        operator_node->getTypeAndName());
    }
}

ASTPtr makePresenceMask(ASTPtr values)
{
    auto lambda = makeASTFunction(
        "lambda",
        makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")));

    return makeASTFunction(
        "groupBitOrForEach",
        makeASTFunction("arrayMap", std::move(lambda), std::move(values)));
}

ASTPtr makeJoinKeyColumn(String & side, ASTPtr && join_group, ConverterContext & context)
{
    if (!isZeroGroupAST(join_group))
        return std::move(join_group);

    SelectQueryBuilder builder;
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
    join_group->setAlias(ColumnNames::JoinGroup);
    builder.select_list.push_back(std::move(join_group));
    builder.from_table = side;

    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), builder.getSelectQuery(), SQLSubqueryType::TABLE});
    side = context.subqueries.back().name;
    return make_intrusive<ASTIdentifier>(Strings{side, ColumnNames::JoinGroup});
}

}
