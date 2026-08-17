#include <Storages/TimeSeries/PrometheusQueryToSQL/zeroGroup.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{

bool isZeroGroupAST(const ASTPtr & group)
{
    if (const auto * literal = group->as<const ASTLiteral>())
        return literal->value == Field{0u};
    if (const auto * function = group->as<const ASTFunction>(); function && (function->name == "CAST") && function->arguments
        && (function->arguments->children.size() == 2))
    {
        const auto * value = function->arguments->children[0]->as<const ASTLiteral>();
        const auto * type = function->arguments->children[1]->as<const ASTLiteral>();
        return value && type && (value->value == Field{0u}) && (type->value == Field{"UInt64"});
    }
    return false;
}


bool producesConstantZeroGroup(const ASTPtr & select_query)
{
    if (!select_query)
        return false;

    const auto * select_with_union = select_query->as<const ASTSelectWithUnionQuery>();
    if (!select_with_union || !select_with_union->list_of_selects || (select_with_union->list_of_selects->children.size() != 1))
        return false;

    const auto * select = select_with_union->list_of_selects->children[0]->as<const ASTSelectQuery>();
    if (!select)
        return false;

    const auto & select_expression_list = select->select();
    if (!select_expression_list)
        return false;

    for (const auto & child : select_expression_list->children)
    {
        if (child->tryGetAlias() == ColumnNames::Group)
            return isZeroGroupAST(child);
    }

    return false;
}

}
