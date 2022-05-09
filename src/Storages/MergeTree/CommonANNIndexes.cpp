#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <Storages/MergeTree/CommonANNIndexes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

namespace ANNCondition
{

ANNCondition::ANNCondition(const SelectQueryInfo & query_info,
                                 ContextPtr context)
{
    // Initialize
    block_with_constants = KeyCondition::
    getBlockWithConstants(query_info.query, query_info.syntax_analyzer_result, context);
    // Build rpns for query sections
    buildRPN(query_info);
    // Match rpns with supported types
    index_is_useful = matchAllRPNS();
}

bool ANNCondition::alwaysUnknownOrTrue(String metric_name) const
{
    if (!index_is_useful)
    {
        return true; // Query isn't supported
    }
    // If query is supported, check metrics for match
    return !(metric_name == ann_expr->metric_name);
}

float ANNCondition::getComparisonDistance() const
{
    if (where_query_type)
    {
        return ann_expr->distance;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not supported method for this query type");
}

std::vector<float> ANNCondition::getTargetVector() const
{
    return ann_expr->target;
}

String ANNCondition::getColumnName() const
{
    return ann_expr->column_name;
}

String ANNCondition::getMetric() const
{
    return ann_expr->metric_name;
}

size_t ANNCondition::getSpaceDim() const
{
    return ann_expr->target.size();
}

float ANNCondition::getPForLpDistance() const
{
    return ann_expr->p_for_lp_dist;
}

bool ANNCondition::queryHasWhereClause() const
{
    return where_query_type;
}

bool ANNCondition::queryHasOrderByClause() const
{
    return order_by_query_type && has_limit;
}

std::optional<UInt64> ANNCondition::getLimitCount() const
{
    return has_limit ? std::optional<UInt64>(limit_expr->length) : std::nullopt;
}

String ANNCondition::getSettingsStr() const
{
    return ann_index_params;
}

void ANNCondition::buildRPN(const SelectQueryInfo & query)
{
    const auto & select = query.query->as<ASTSelectQuery &>();

    if (select.prewhere()) // If query has PREWHERE section
    {
        traverseAST(select.prewhere(), rpn_prewhere_clause);
    }

    if (select.where()) // If query has WHERE section
    {
        traverseAST(select.where(), rpn_where_clause);
    }

    if (select.limitLength()) // If query has LIMIT section
    {
        traverseAST(select.limitLength(), rpn_limit_clause);
    }

    if (select.settings()) // If query has SETTINGS section
    {
        parseSettings(select.settings());
    }

    if (select.orderBy()) // If query has ORDERBY section
    {
        traverseOrderByAST(select.orderBy(), rpn_order_by_clause);
    }

    // Reverse RPNs for conveniences during parsing
    std::reverse(rpn_prewhere_clause.begin(), rpn_prewhere_clause.end());
    std::reverse(rpn_where_clause.begin(), rpn_where_clause.end());
    std::reverse(rpn_order_by_clause.begin(), rpn_order_by_clause.end());
}

void ANNCondition::traverseAST(const ASTPtr & node, RPN & rpn)
{
    // If the node is ASTFUunction, it may have children nodes
    if (const auto * func = node->as<ASTFunction>())
    {
        const ASTs & args = func->arguments->children;
        // Traverse children nodes
        for (const auto& arg : args)
        {
            traverseAST(arg, rpn);
        }
    }

    RPNElement element;
    // Get the data behind node
    if (!traverseAtomAST(node, element))
    {
        element.function = RPNElement::FUNCTION_UNKNOWN;
    }

    rpn.emplace_back(std::move(element));
}

bool ANNCondition::traverseAtomAST(const ASTPtr & node, RPNElement & out)
{
    // Match Functions
    if (const auto * function = node->as<ASTFunction>())
    {
        // Set the name
        out.func_name = function->name;

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
    return tryCastToConstType(node, out);
 }

bool ANNCondition::tryCastToConstType(const ASTPtr & node, RPNElement & out)
{
    Field const_value;
    DataTypePtr const_type;

    if (KeyCondition::getConstant(node, block_with_constants, const_value, const_type))
    {
        /// Check for constant types
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
        if (const_value.getType() == Field::Types::Tuple)
        {
            out.function = RPNElement::FUNCTION_LITERAL_TUPLE;
            out.tuple_literal = const_value.get<Tuple>();
            out.func_name = "Tuple literal";
            return true;
        }
    }

    return false;
}

void ANNCondition::traverseOrderByAST(const ASTPtr & node, RPN & rpn)
{
    if (const auto * expr_list = node->as<ASTExpressionList>())
    {
        if (const auto * order_by_element = expr_list->children.front()->as<ASTOrderByElement>())
        {
            traverseAST(order_by_element->children.front(), rpn);
        }
    }
}

bool ANNCondition::matchAllRPNS()
{
    ANNExpression expr_prewhere;
    ANNExpression expr_where;
    ANNExpression expr_order_by;
    LimitExpression expr_limit;
    bool prewhere_is_valid = matchRPNWhere(rpn_prewhere_clause, expr_prewhere);
    bool where_is_valid = matchRPNWhere(rpn_where_clause, expr_where);
    bool limit_is_valid = matchRPNLimit(rpn_limit_clause, expr_limit);
    bool order_by_is_valid = matchRPNOrderBy(rpn_order_by_clause, expr_order_by);

    // Search type query in both sections isn't supported
    if (prewhere_is_valid && where_is_valid)
    {
        return false;
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
        return false;
    }

    return where_query_type || (has_limit && order_by_query_type);
}

bool ANNCondition::matchRPNLimit(RPN & rpn, LimitExpression & expr)
{
    // LIMIT section must have least 1 expression
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

void ANNCondition::parseSettings(const ASTPtr & node)
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
}

bool ANNCondition::matchRPNOrderBy(RPN & rpn, ANNExpression & expr)
{
    // ORDERBY section must have at least 3 expressions
    if (rpn.size() < 3)
    {
        return false;
    }

    auto iter = rpn.begin();
    auto end = rpn.end();
    bool identifier_found = false;

    return ANNCondition::matchMainParts(iter, end, expr, identifier_found);
}

bool ANNCondition::matchMainParts(RPN::iterator & iter, RPN::iterator & end,
     ANNExpression & expr, bool & identifier_found)
     {
    // Matches DistanceFunc->[Column]->[TupleFunc]->TargetVector(floats)->[Column]
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

bool ANNCondition::matchRPNWhere(RPN & rpn, ANNExpression & expr)
{
    // WHERE section must have at least 5 expressions
    // Operator->Distance(float)->DistanceFunc->Column->TupleFunc(TargetVector(floats))
    if (rpn.size() < 5)
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

String ANNCondition::getIdentifierOrPanic(RPN::iterator& iter)
{
    String identifier;
    try
    {
        identifier = std::move(iter->identifier.value());
    }
    catch (...)
    {
        ANNCondition::panicIfWrongBuiltRPN();
    }
    return identifier;
}

float ANNCondition::getFloatOrIntLiteralOrPanic(RPN::iterator& iter)
{
    if (iter->float_literal.has_value())
    {
        return iter->float_literal.value();
    }
    if (iter->int_literal.has_value())
    {
        return static_cast<float>(iter->int_literal.value());
    }
    ANNCondition::panicIfWrongBuiltRPN();
}

void ANNCondition::panicIfWrongBuiltRPN()
{
    LOG_DEBUG(&Poco::Logger::get("ANNCondition"), "Wrong parsing of AST");
    throw Exception(
                "Wrong parsed AST in buildRPN\n", DB::ErrorCodes::INCORRECT_QUERY);
}

}

}
