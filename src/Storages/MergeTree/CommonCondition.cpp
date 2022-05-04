#include <cstddef>
#include <optional>
#include <Parsers/ASTFunction.h>

#include "Core/Block.h"
#include "Core/Field.h"
#include "IO/ReadBuffer.h"
#include "Interpreters/Context_fwd.h"
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTFunctionWithKeyValueArguments.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTIdentifier_fwd.h"
#include "Parsers/ASTLiteral.h"
#include "Parsers/ASTOrderByElement.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/ASTSetQuery.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include "Parsers/Access/ASTCreateUserQuery.h"
#include "Parsers/Access/ASTRolesOrUsersSet.h"
#include "Parsers/Access/ASTSettingsProfileElement.h"
#include "Parsers/IAST_fwd.h"

#include <Storages/MergeTree/CommonCondition.h>
#include <Storages/MergeTree/KeyCondition.h>

#include "Storages/SelectQueryInfo.h"
#include "base/logger_useful.h"
#include "base/types.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Condition
{

CommonCondition::CommonCondition(const SelectQueryInfo & query_info,
                                 ContextPtr context)
{
    buildRPN(query_info, context);
    index_is_useful = matchAllRPNS();
}

bool CommonCondition::alwaysUnknownOrTrue() const
{
    return !index_is_useful;
}

float CommonCondition::getComparisonDistance() const
{
    if (where_query_type)
    {
        return ann_expr->distance;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not supported method for this query type");
}

std::vector<float> CommonCondition::getTargetVector() const
{
    return ann_expr->target;
}

String CommonCondition::getColumnName() const
{
    return ann_expr->column_name;
}

String CommonCondition::getMetric() const
{
    return ann_expr->metric_name;
}

size_t CommonCondition::getSpaceDim() const
{
    return ann_expr->target.size();
}

float CommonCondition::getPForLpDistance() const
{
    return ann_expr->p_for_lp_dist;
}

bool CommonCondition::queryHasWhereClause() const
{
    return where_query_type;
}

bool CommonCondition::queryHasOrderByClause() const
{
    return order_by_query_type && has_limit;
}

std::optional<UInt64> CommonCondition::getLimitLength() const
{
    return has_limit ? std::optional<UInt64>(limit_expr->length) : std::nullopt;
}

String CommonCondition::getSettingsStr() const
{
    return ann_index_params;
}

void CommonCondition::buildRPN(const SelectQueryInfo & query, ContextPtr context)
{
    block_with_constants = KeyCondition::getBlockWithConstants(query.query, query.syntax_analyzer_result, context);

    const auto & select = query.query->as<ASTSelectQuery &>();

    if (select.prewhere())
    {
        traverseAST(select.prewhere(), rpn_prewhere_clause);
    }

    if (select.where())
    {
        traverseAST(select.where(), rpn_where_clause);
    }

    if (select.limitLength())
    {
        traverseAST(select.limitLength(), rpn_limit_clause);
    }

    if (select.settings())
    {
        parseSettings(select.settings());
    }

    if (select.orderBy())
    {
        if (const auto * expr_list = select.orderBy()->as<ASTExpressionList>())
        {
            if (const auto * order_by_element = expr_list->children.front()->as<ASTOrderByElement>())
            {
                traverseAST(order_by_element->children.front(), rpn_order_by_clause);
            }
        }
    }

    std::reverse(rpn_prewhere_clause.begin(), rpn_prewhere_clause.end());
    std::reverse(rpn_where_clause.begin(), rpn_where_clause.end());
    std::reverse(rpn_order_by_clause.begin(), rpn_order_by_clause.end());
}

void CommonCondition::traverseAST(const ASTPtr & node, RPN & rpn)
{
    if (const auto * func = node->as<ASTFunction>())
    {
        const ASTs & args = func->arguments->children;

        for (const auto& arg : args)
        {
            traverseAST(arg, rpn);
        }
    }

    RPNElement element;

    if (!traverseAtomAST(node, element))
    {
        element.function = RPNElement::FUNCTION_UNKNOWN;
    }

    rpn.emplace_back(std::move(element));
}

bool CommonCondition::traverseAtomAST(const ASTPtr & node, RPNElement & out)
{

    if (const auto * order_by_element = node->as<ASTOrderByElement>())
    {
        out.function = RPNElement::FUNCTION_ORDER_BY_ELEMENT;
        out.func_name = "order by elemnet";

        return true;
    }

    if (const auto * function = node->as<ASTFunction>())
    {
        // Set the name
        out.func_name = function->name;

        // TODO: Add support for LpDistance
        if (function->name == "L1Distance" ||
            function->name == "L2Distance" ||
            function->name == "LinfDistance" ||
            function->name == "cosineDistance" ||
            function->name == "dotProduct" ||
            function->name == "LpDistance")
        {
            out.function = RPNElement::FUNCTION_DISTANCE;
        }
        else if (function->name == "tuple")
        {
            out.function = RPNElement::FUNCTION_TUPLE;
        }
        else if (function->name == "less" ||
                 function->name == "greater" ||
                 function->name == "lessOrEquals" ||
                 function->name == "greaterOrEquals")
        {
            out.function = RPNElement::FUNCTION_COMPARISON;
        }
        else
        {
            return false;
        }

        return true;
    }
    // Match identifier 
    else if (const auto * identifier = node->as<ASTIdentifier>())
    {
        out.function = RPNElement::FUNCTION_IDENTIFIER;
        out.identifier.emplace(identifier->name());
        out.func_name = "column identifier";

        return true;
    }

     // Check if we have constants behind the node
    {
        Field const_value;
        DataTypePtr const_type;

        if (KeyCondition::getConstant(node, block_with_constants, const_value, const_type))
        {
            /// Check constant type (use Float64 because all Fields implementation contains Float64 (for Float32 too))
            if (const_value.getType() == Field::Types::Float64)
            {
                out.function = RPNElement::FUNCTION_FLOAT_LITERAL;
                out.float_literal.emplace(const_value.get<Float32>());
                out.func_name = "Float literal";
                return true;
            }
            if (const_value.getType() == Field::Types::UInt64)
            {
                out.function = RPNElement::FUNCTION_INT_LITERAL;
                out.int_literal.emplace(const_value.get<UInt64>());
                out.func_name = "Int literal";
                return true;
            }
            if (const_value.getType() == Field::Types::Int64)
            {
                out.function = RPNElement::FUNCTION_INT_LITERAL;
                out.int_literal.emplace(const_value.get<Int64>());
                out.func_name = "Int literal";
                return true;
            }
            if (const_value.getType() == Field::Types::String)
            {
                out.function = RPNElement::FUNCTION_STRING;
                out.identifier.emplace(const_value.get<String>());
                out.func_name = "setting string";
                return true;
            }
            if (const_value.getType() == Field::Types::Tuple)
            {
                out.function = RPNElement::FUNCTION_LITERAL_TUPLE;
                out.tuple_literal = const_value.get<Tuple>();
                out.func_name = "Tuple literal";
                return true;
            }
        }
    }

    return false;
 }

bool CommonCondition::matchAllRPNS()
{
    ANNExpression expr_prewhere;
    ANNExpression expr_where;
    ANNExpression expr_order_by;
    LimitExpression expr_limit;
    bool prewhere_is_valid = matchRPNWhere(rpn_prewhere_clause, expr_prewhere);
    bool where_is_valid = matchRPNWhere(rpn_where_clause, expr_where);
    bool limit_is_valid = matchRPNLimit(rpn_limit_clause, expr_limit);
    bool order_by_is_valid = matchRPNOrderBy(rpn_order_by_clause, expr_order_by);

    // Unxpected situation
    if (prewhere_is_valid && where_is_valid)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Have both where and prewhere valid clauses - is not supported");
    }

    if (prewhere_is_valid || where_is_valid)
    {
        ann_expr = std::move(where_is_valid ? expr_where : expr_prewhere);
        where_query_type = true;
    }
    if (order_by_is_valid)
    {
        ann_expr = std::move(expr_order_by);
        order_by_query_type = true;
    }
    if (limit_is_valid)
    {
        limit_expr = std::move(expr_limit);
        has_limit = true;
    }

    if (where_query_type && (has_limit && order_by_query_type))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
        "The query with Valid Where Clause and valid OrderBy clause - is not supported");
    }

    return where_query_type || (has_limit && order_by_query_type);
}

bool CommonCondition::matchRPNLimit(RPN & rpn, LimitExpression & expr)
{
    if (rpn.size() != 1)
    {
        return false;
    }
    if (rpn.front().function == RPNElement::FUNCTION_INT_LITERAL)
    {
        expr.length = rpn.front().int_literal.value();
        return true;
    }
    return false;
}

void CommonCondition::parseSettings(const ASTPtr & node)
{
    if (const auto * set = node->as<ASTSetQuery>())
    {
        for (const auto & change : set->changes)
        {
            if (change.name == "ann_index_params")
            {
                ann_index_params = change.value.get<String>();
                return;
            }
        }
    }
    ann_index_params = "";
}

bool CommonCondition::matchRPNOrderBy(RPN & rpn, ANNExpression & expr)
{
    if (rpn.size() < 3)
    {
        return false;
    }

    auto iter = rpn.begin();
    auto end = rpn.end();
    bool identifier_found = false;

    return CommonCondition::matchMainParts(iter, end, expr, identifier_found);
}

bool CommonCondition::matchMainParts(RPN::iterator & iter, RPN::iterator & end,
     ANNExpression & expr, bool & identifier_found)
     {

    if (iter->function != RPNElement::FUNCTION_DISTANCE)
    {
        return false;
    }

    expr.metric_name = iter->func_name;
    ++iter;

    if (expr.metric_name == "LpDistance")
    {
        if (iter->function != RPNElement::FUNCTION_FLOAT_LITERAL &&
            iter->function != RPNElement::FUNCTION_INT_LITERAL)
        {
            return false;
        }
        expr.p_for_lp_dist = getFloatOrIntLiteralOrPanic(iter);
        ++iter;
    }


    if (iter->function == RPNElement::FUNCTION_IDENTIFIER)
    {
        identifier_found = true;
        expr.column_name = getIdentifierOrPanic(iter);
        ++iter;
    }

    if (iter->function == RPNElement::FUNCTION_TUPLE)
    {
        ++iter;
    }

    if (iter->function == RPNElement::FUNCTION_LITERAL_TUPLE)
    {
        for (const auto & value : iter->tuple_literal.value())
        {
            expr.target.emplace_back(value.get<float>());
        }
        ++iter;
    }


    while (iter != end)
    {
        if (iter->function == RPNElement::FUNCTION_FLOAT_LITERAL ||
            iter->function == RPNElement::FUNCTION_INT_LITERAL)
        {
            expr.target.emplace_back(getFloatOrIntLiteralOrPanic(iter));
        }
        else if (iter->function == RPNElement::FUNCTION_IDENTIFIER)
        {
            if (identifier_found)
            {
                return false;
            }
            expr.column_name = getIdentifierOrPanic(iter);
            identifier_found = true;
        }
        else
        {
            return false;
        }

        ++iter;
    }

    return true;
}

bool CommonCondition::matchRPNWhere(RPN & rpn, ANNExpression & expr)
{
    const size_t minimal_elemets_count = 6;// At least 6 AST nodes in querry
    if (rpn.size() < minimal_elemets_count)
    {
        return false;
    }

    auto iter = rpn.begin();
    bool identifier_found = false;

    // Query starts from operator less
    if (iter->function != RPNElement::FUNCTION_COMPARISON)
    {
        return false;
    }

    const bool greater_case = iter->func_name == "greater" || iter->func_name == "greaterOrEquals";
    const bool less_case = iter->func_name == "less" || iter->func_name == "lessOrEquals";

    ++iter;

    if (less_case)
    {
        if (iter->function != RPNElement::FUNCTION_FLOAT_LITERAL)
        {
            return false;
        }

        expr.distance = getFloatOrIntLiteralOrPanic(iter);
        ++iter;

    }
    else if (!greater_case)
    {
        return false;
    }

    auto end = rpn.end();
    if (!matchMainParts(iter, end, expr, identifier_found))
    {
        return false;
    }

    // Final checks of correctness

    if (!identifier_found || expr.target.empty())
    {
        return false;
    }

    if (greater_case)
    {
        if (expr.target.size() < 2)
        {
            return false;
        }
        expr.distance = expr.target.back();
        expr.target.pop_back();
    }

    // Querry is ok
    return true;
}

String CommonCondition::getIdentifierOrPanic(RPN::iterator& iter)
{
    String identifier;
    try
    {
        identifier = std::move(iter->identifier.value());
    }
    catch (...)
    {
        CommonCondition::panicIfWrongBuiltRPN();
    }
    return identifier;
}

float CommonCondition::getFloatOrIntLiteralOrPanic(RPN::iterator& iter)
{
    if (iter->float_literal.has_value())
    {
        return iter->float_literal.value();
    }
    if (iter->int_literal.has_value())
    {
        return static_cast<float>(iter->int_literal.value());
    }
    CommonCondition::panicIfWrongBuiltRPN();
}

void CommonCondition::panicIfWrongBuiltRPN()
{
    LOG_DEBUG(&Poco::Logger::get("CommonCondition"), "Wrong parsing of AST");
    throw Exception(
                "Wrong parsed AST in buildRPN\n", DB::ErrorCodes::LOGICAL_ERROR);
}

}

}
