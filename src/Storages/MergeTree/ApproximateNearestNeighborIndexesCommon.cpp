#include <Storages/MergeTree/ApproximateNearestNeighborIndexesCommon.h>

#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

namespace
{

template <typename Literal>
void extractReferenceVectorFromLiteral(ApproximateNearestNeighborInformation::Embedding & reference_vector, Literal literal)
{
    Float64 float_element_of_reference_vector;
    Int64 int_element_of_reference_vector;

    for (const auto & value : literal.value())
    {
        if (value.tryGet(float_element_of_reference_vector))
            reference_vector.emplace_back(float_element_of_reference_vector);
        else if (value.tryGet(int_element_of_reference_vector))
            reference_vector.emplace_back(static_cast<float>(int_element_of_reference_vector));
        else
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Wrong type of elements in reference vector. Only float or int are supported.");
    }
}

ApproximateNearestNeighborInformation::Metric stringToMetric(std::string_view metric)
{
    if (metric == "L2Distance")
        return ApproximateNearestNeighborInformation::Metric::L2;
    else if (metric == "LpDistance")
        return ApproximateNearestNeighborInformation::Metric::Lp;
    else
        return ApproximateNearestNeighborInformation::Metric::Unknown;
}

}

ApproximateNearestNeighborCondition::ApproximateNearestNeighborCondition(const SelectQueryInfo & query_info, ContextPtr context)
    : block_with_constants(KeyCondition::getBlockWithConstants(query_info.query, query_info.syntax_analyzer_result, context))
    , index_granularity(context->getMergeTreeSettings().index_granularity)
    , max_limit_for_ann_queries(context->getSettings().max_limit_for_ann_queries)
    , index_is_useful(checkQueryStructure(query_info))
{}

bool ApproximateNearestNeighborCondition::alwaysUnknownOrTrue(String metric) const
{
    if (!index_is_useful)
        return true; // Query isn't supported
    // If query is supported, check metrics for match
    return !(stringToMetric(metric) == query_information->metric);
}

float ApproximateNearestNeighborCondition::getComparisonDistanceForWhereQuery() const
{
    if (index_is_useful && query_information.has_value()
        && query_information->type == ApproximateNearestNeighborInformation::Type::Where)
        return query_information->distance;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not supported method for this query type");
}

UInt64 ApproximateNearestNeighborCondition::getLimit() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->limit;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "No LIMIT section in query, not supported");
}

std::vector<float> ApproximateNearestNeighborCondition::getReferenceVector() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->reference_vector;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Reference vector was requested for useless or uninitialized index.");
}

size_t ApproximateNearestNeighborCondition::getDimensions() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->reference_vector.size();
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of dimensions was requested for useless or uninitialized index.");
}

String ApproximateNearestNeighborCondition::getColumnName() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->column_name;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column name was requested for useless or uninitialized index.");
}

ApproximateNearestNeighborInformation::Metric ApproximateNearestNeighborCondition::getMetricType() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->metric;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Metric name was requested for useless or uninitialized index.");
}

float ApproximateNearestNeighborCondition::getPValueForLpDistance() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->p_for_lp_dist;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "P from LPDistance was requested for useless or uninitialized index.");
}

ApproximateNearestNeighborInformation::Type ApproximateNearestNeighborCondition::getQueryType() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->type;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Query type was requested for useless or uninitialized index.");
}

bool ApproximateNearestNeighborCondition::checkQueryStructure(const SelectQueryInfo & query)
{
    /// RPN-s for different sections of the query
    RPN rpn_prewhere_clause;
    RPN rpn_where_clause;
    RPN rpn_order_by_clause;
    RPNElement rpn_limit;
    UInt64 limit;

    ApproximateNearestNeighborInformation prewhere_info;
    ApproximateNearestNeighborInformation where_info;
    ApproximateNearestNeighborInformation order_by_info;

    /// Build rpns for query sections
    const auto & select = query.query->as<ASTSelectQuery &>();

    /// If query has PREWHERE clause
    if (select.prewhere())
        traverseAST(select.prewhere(), rpn_prewhere_clause);

    /// If query has WHERE clause
    if (select.where())
        traverseAST(select.where(), rpn_where_clause);

    /// If query has LIMIT clause
    if (select.limitLength())
        traverseAtomAST(select.limitLength(), rpn_limit);

    if (select.orderBy()) // If query has ORDERBY clause
        traverseOrderByAST(select.orderBy(), rpn_order_by_clause);

    /// Reverse RPNs for conveniences during parsing
    std::reverse(rpn_prewhere_clause.begin(), rpn_prewhere_clause.end());
    std::reverse(rpn_where_clause.begin(), rpn_where_clause.end());
    std::reverse(rpn_order_by_clause.begin(), rpn_order_by_clause.end());

    /// Match rpns with supported types and extract information
    const bool prewhere_is_valid = matchRPNWhere(rpn_prewhere_clause, prewhere_info);
    const bool where_is_valid = matchRPNWhere(rpn_where_clause, where_info);
    const bool order_by_is_valid = matchRPNOrderBy(rpn_order_by_clause, order_by_info);
    const bool limit_is_valid = matchRPNLimit(rpn_limit, limit);

    /// Query without a LIMIT clause or with a limit greater than a restriction is not supported
    if (!limit_is_valid || max_limit_for_ann_queries < limit)
        return false;

    /// Search type query in both sections isn't supported
    if (prewhere_is_valid && where_is_valid)
        return false;

    /// Search type should be in WHERE or PREWHERE clause
    if (prewhere_is_valid || where_is_valid)
        query_information = std::move(prewhere_is_valid ? prewhere_info : where_info);

    if (order_by_is_valid)
    {
        /// Query with valid where and order by type is not supported
        if (query_information.has_value())
            return false;

        query_information = std::move(order_by_info);
    }

    if (query_information)
        query_information->limit = limit;

    return query_information.has_value();
}

void ApproximateNearestNeighborCondition::traverseAST(const ASTPtr & node, RPN & rpn)
{
    // If the node is ASTFunction, it may have children nodes
    if (const auto * func = node->as<ASTFunction>())
    {
        const ASTs & children = func->arguments->children;
        // Traverse children nodes
        for (const auto& child : children)
            traverseAST(child, rpn);
    }

    RPNElement element;
    /// Get the data behind node
    if (!traverseAtomAST(node, element))
        element.function = RPNElement::FUNCTION_UNKNOWN;

    rpn.emplace_back(std::move(element));
}

bool ApproximateNearestNeighborCondition::traverseAtomAST(const ASTPtr & node, RPNElement & out)
{
    /// Match Functions
    if (const auto * function = node->as<ASTFunction>())
    {
        /// Set the name
        out.func_name = function->name;

        if (function->name == "L1Distance" ||
            function->name == "L2Distance" ||
            function->name == "LinfDistance" ||
            function->name == "cosineDistance" ||
            function->name == "dotProduct" ||
            function->name == "LpDistance")
            out.function = RPNElement::FUNCTION_DISTANCE;
        else if (function->name == "tuple")
            out.function = RPNElement::FUNCTION_TUPLE;
        else if (function->name == "array")
            out.function = RPNElement::FUNCTION_ARRAY;
        else if (function->name == "less" ||
                 function->name == "greater" ||
                 function->name == "lessOrEquals" ||
                 function->name == "greaterOrEquals")
            out.function = RPNElement::FUNCTION_COMPARISON;
        else if (function->name == "_CAST")
            out.function = RPNElement::FUNCTION_CAST;
        else
            return false;

        return true;
    }
    /// Match identifier
    else if (const auto * identifier = node->as<ASTIdentifier>())
    {
        out.function = RPNElement::FUNCTION_IDENTIFIER;
        out.identifier.emplace(identifier->name());
        out.func_name = "column identifier";

        return true;
    }

    /// Check if we have constants behind the node
    return tryCastToConstType(node, out);
}

bool ApproximateNearestNeighborCondition::tryCastToConstType(const ASTPtr & node, RPNElement & out)
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

        if (const_value.getType() == Field::Types::Array)
        {
            out.function = RPNElement::FUNCTION_LITERAL_ARRAY;
            out.array_literal = const_value.get<Array>();
            out.func_name = "Array literal";
            return true;
        }

        if (const_value.getType() == Field::Types::String)
        {
            out.function = RPNElement::FUNCTION_STRING_LITERAL;
            out.func_name = const_value.get<String>();
            return true;
        }
    }

    return false;
}

void ApproximateNearestNeighborCondition::traverseOrderByAST(const ASTPtr & node, RPN & rpn)
{
    if (const auto * expr_list = node->as<ASTExpressionList>())
        if (const auto * order_by_element = expr_list->children.front()->as<ASTOrderByElement>())
            traverseAST(order_by_element->children.front(), rpn);
}

/// Returns true and stores ApproximateNearestNeighborInformation if the query has valid WHERE clause
bool ApproximateNearestNeighborCondition::matchRPNWhere(RPN & rpn, ApproximateNearestNeighborInformation & ann_info)
{
    /// Fill query type field
    ann_info.type = ApproximateNearestNeighborInformation::Type::Where;

    /// WHERE section must have at least 5 expressions
    /// Operator->Distance(float)->DistanceFunc->Column->Tuple(Array)Func(ReferenceVector(floats))
    if (rpn.size() < 5)
        return false;

    auto iter = rpn.begin();

    /// Query starts from operator less
    if (iter->function != RPNElement::FUNCTION_COMPARISON)
        return false;

    const bool greater_case = iter->func_name == "greater" || iter->func_name == "greaterOrEquals";
    const bool less_case = iter->func_name == "less" || iter->func_name == "lessOrEquals";

    ++iter;

    if (less_case)
    {
        if (iter->function != RPNElement::FUNCTION_FLOAT_LITERAL)
            return false;

        ann_info.distance = getFloatOrIntLiteralOrPanic(iter);
        if (ann_info.distance < 0)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Distance can't be negative. Got {}", ann_info.distance);

        ++iter;

    }
    else if (!greater_case)
        return false;

    auto end = rpn.end();
    if (!matchMainParts(iter, end, ann_info))
        return false;

    if (greater_case)
    {
        if (ann_info.reference_vector.size() < 2)
            return false;
        ann_info.distance = ann_info.reference_vector.back();
        if (ann_info.distance < 0)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Distance can't be negative. Got {}", ann_info.distance);
        ann_info.reference_vector.pop_back();
    }

    /// query is ok
    return true;
}

/// Returns true and stores ANNExpr if the query has valid ORDERBY clause
bool ApproximateNearestNeighborCondition::matchRPNOrderBy(RPN & rpn, ApproximateNearestNeighborInformation & ann_info)
{
    /// Fill query type field
    ann_info.type = ApproximateNearestNeighborInformation::Type::OrderBy;

    // ORDER BY clause must have at least 3 expressions
    if (rpn.size() < 3)
        return false;

    auto iter = rpn.begin();
    auto end = rpn.end();

    return ApproximateNearestNeighborCondition::matchMainParts(iter, end, ann_info);
}

/// Returns true and stores Length if we have valid LIMIT clause in query
bool ApproximateNearestNeighborCondition::matchRPNLimit(RPNElement & rpn, UInt64 & limit)
{
    if (rpn.function == RPNElement::FUNCTION_INT_LITERAL)
    {
        limit = rpn.int_literal.value();
        return true;
    }

    return false;
}

/// Matches dist function, referencer vector, column name
bool ApproximateNearestNeighborCondition::matchMainParts(RPN::iterator & iter, const RPN::iterator & end, ApproximateNearestNeighborInformation & ann_info)
{
    bool identifier_found = false;

    /// Matches DistanceFunc->[Column]->[Tuple(array)Func]->ReferenceVector(floats)->[Column]
    if (iter->function != RPNElement::FUNCTION_DISTANCE)
        return false;

    ann_info.metric = stringToMetric(iter->func_name);
    ++iter;

    if (ann_info.metric == ApproximateNearestNeighborInformation::Metric::Lp)
    {
        if (iter->function != RPNElement::FUNCTION_FLOAT_LITERAL &&
            iter->function != RPNElement::FUNCTION_INT_LITERAL)
            return false;
        ann_info.p_for_lp_dist = getFloatOrIntLiteralOrPanic(iter);
        ++iter;
    }

    if (iter->function == RPNElement::FUNCTION_IDENTIFIER)
    {
        identifier_found = true;
        ann_info.column_name = std::move(iter->identifier.value());
        ++iter;
    }

    if (iter->function == RPNElement::FUNCTION_TUPLE || iter->function == RPNElement::FUNCTION_ARRAY)
        ++iter;

    if (iter->function == RPNElement::FUNCTION_LITERAL_TUPLE)
    {
        extractReferenceVectorFromLiteral(ann_info.reference_vector, iter->tuple_literal);
        ++iter;
    }

    if (iter->function == RPNElement::FUNCTION_LITERAL_ARRAY)
    {
        extractReferenceVectorFromLiteral(ann_info.reference_vector, iter->array_literal);
        ++iter;
    }

    /// further conditions are possible if there is no tuple or array, or no identifier is found
    /// the tuple or array can be inside a cast function. For other cases, see the loop after this condition
    if (iter != end && iter->function == RPNElement::FUNCTION_CAST)
    {
        ++iter;
        /// Cast should be made to array or tuple
        if (!iter->func_name.starts_with("Array") && !iter->func_name.starts_with("Tuple"))
            return false;
        ++iter;
        if (iter->function == RPNElement::FUNCTION_LITERAL_TUPLE)
        {
            extractReferenceVectorFromLiteral(ann_info.reference_vector, iter->tuple_literal);
            ++iter;
        }
        else if (iter->function == RPNElement::FUNCTION_LITERAL_ARRAY)
        {
            extractReferenceVectorFromLiteral(ann_info.reference_vector, iter->array_literal);
            ++iter;
        }
        else
            return false;
    }

    while (iter != end)
    {
        if (iter->function == RPNElement::FUNCTION_FLOAT_LITERAL ||
            iter->function == RPNElement::FUNCTION_INT_LITERAL)
            ann_info.reference_vector.emplace_back(getFloatOrIntLiteralOrPanic(iter));
        else if (iter->function == RPNElement::FUNCTION_IDENTIFIER)
        {
            if (identifier_found)
                return false;
            ann_info.column_name = std::move(iter->identifier.value());
            identifier_found = true;
        }
        else
            return false;

        ++iter;
    }

    /// Final checks of correctness
    return identifier_found && !ann_info.reference_vector.empty();
}

/// Gets float or int from AST node
float ApproximateNearestNeighborCondition::getFloatOrIntLiteralOrPanic(const RPN::iterator& iter)
{
    if (iter->float_literal.has_value())
        return iter->float_literal.value();
    if (iter->int_literal.has_value())
        return static_cast<float>(iter->int_literal.value());
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Wrong parsed AST in buildRPN\n");
}

}
