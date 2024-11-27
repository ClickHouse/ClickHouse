#include <Storages/MergeTree/VectorSimilarityCondition.h>

#include <Core/Settings.h>
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
namespace Setting
{
    extern const SettingsUInt64 max_limit_for_ann_queries;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

namespace
{

template <typename Literal>
void extractReferenceVectorFromLiteral(std::vector<Float64> & reference_vector, Literal literal)
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

VectorSimilarityCondition::Info::DistanceFunction stringToDistanceFunction(const String & distance_function)
{
    if (distance_function == "L2Distance")
        return VectorSimilarityCondition::Info::DistanceFunction::L2;
    if (distance_function == "cosineDistance")
        return VectorSimilarityCondition::Info::DistanceFunction::Cosine;
    return VectorSimilarityCondition::Info::DistanceFunction::Unknown;
}

}

VectorSimilarityCondition::VectorSimilarityCondition(const SelectQueryInfo & query_info, ContextPtr context)
    : block_with_constants(KeyCondition::getBlockWithConstants(query_info.query, query_info.syntax_analyzer_result, context))
    , max_limit_for_ann_queries(context->getSettingsRef()[Setting::max_limit_for_ann_queries])
    , index_is_useful(checkQueryStructure(query_info))
{}

bool VectorSimilarityCondition::alwaysUnknownOrTrue(const String & distance_function) const
{
    if (!index_is_useful)
        return true; /// query isn't supported
    /// If query is supported, check if distance function of index is the same as distance function in query
    return !(stringToDistanceFunction(distance_function) == query_information->distance_function);
}

UInt64 VectorSimilarityCondition::getLimit() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->limit;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "No LIMIT section in query, not supported");
}

std::vector<Float64> VectorSimilarityCondition::getReferenceVector() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->reference_vector;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Reference vector was requested for useless or uninitialized index.");
}

size_t VectorSimilarityCondition::getDimensions() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->reference_vector.size();
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of dimensions was requested for useless or uninitialized index.");
}

String VectorSimilarityCondition::getColumnName() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->column_name;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Column name was requested for useless or uninitialized index.");
}

VectorSimilarityCondition::Info::DistanceFunction VectorSimilarityCondition::getDistanceFunction() const
{
    if (index_is_useful && query_information.has_value())
        return query_information->distance_function;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Distance function was requested for useless or uninitialized index.");
}

bool VectorSimilarityCondition::checkQueryStructure(const SelectQueryInfo & query)
{
    Info order_by_info;

    /// Build rpns for query sections
    const auto & select = query.query->as<ASTSelectQuery &>();

    RPN rpn_order_by;
    RPNElement rpn_limit;
    UInt64 limit;

    if (select.limitLength())
        traverseAtomAST(select.limitLength(), rpn_limit);

    if (select.orderBy())
        traverseOrderByAST(select.orderBy(), rpn_order_by);

    /// Reverse RPNs for conveniences during parsing
    std::reverse(rpn_order_by.begin(), rpn_order_by.end());

    const bool order_by_is_valid = matchRPNOrderBy(rpn_order_by, order_by_info);
    const bool limit_is_valid = matchRPNLimit(rpn_limit, limit);

    if (!limit_is_valid || limit > max_limit_for_ann_queries)
        return false;

    if (order_by_is_valid)
    {
        query_information = std::move(order_by_info);
        query_information->limit = limit;
        return true;
    }

    return false;
}

void VectorSimilarityCondition::traverseAST(const ASTPtr & node, RPN & rpn)
{
    /// If the node is ASTFunction, it may have children nodes
    if (const auto * func = node->as<ASTFunction>())
    {
        const ASTs & children = func->arguments->children;
        /// Traverse children nodes
        for (const auto& child : children)
            traverseAST(child, rpn);
    }

    RPNElement element;
    /// Get the data behind node
    if (!traverseAtomAST(node, element))
        element.function = RPNElement::FUNCTION_UNKNOWN;

    rpn.emplace_back(std::move(element));
}

bool VectorSimilarityCondition::traverseAtomAST(const ASTPtr & node, RPNElement & out)
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
            function->name == "dotProduct")
            out.function = RPNElement::FUNCTION_DISTANCE;
        else if (function->name == "array")
            out.function = RPNElement::FUNCTION_ARRAY;
        else if (function->name == "_CAST")
            out.function = RPNElement::FUNCTION_CAST;
        else
            return false;

        return true;
    }
    /// Match identifier
    if (const auto * identifier = node->as<ASTIdentifier>())
    {
        out.function = RPNElement::FUNCTION_IDENTIFIER;
        out.identifier.emplace(identifier->name());
        out.func_name = "column identifier";

        return true;
    }

    /// Check if we have constants behind the node
    return tryCastToConstType(node, out);
}

bool VectorSimilarityCondition::tryCastToConstType(const ASTPtr & node, RPNElement & out)
{
    Field const_value;
    DataTypePtr const_type;

    if (KeyCondition::getConstant(node, block_with_constants, const_value, const_type))
    {
        /// Check for constant types
        if (const_value.getType() == Field::Types::Float64)
        {
            out.function = RPNElement::FUNCTION_FLOAT_LITERAL;
            out.float_literal.emplace(const_value.safeGet<Float32>());
            out.func_name = "Float literal";
            return true;
        }

        if (const_value.getType() == Field::Types::UInt64)
        {
            out.function = RPNElement::FUNCTION_INT_LITERAL;
            out.int_literal.emplace(const_value.safeGet<UInt64>());
            out.func_name = "Int literal";
            return true;
        }

        if (const_value.getType() == Field::Types::Int64)
        {
            out.function = RPNElement::FUNCTION_INT_LITERAL;
            out.int_literal.emplace(const_value.safeGet<Int64>());
            out.func_name = "Int literal";
            return true;
        }

        if (const_value.getType() == Field::Types::Array)
        {
            out.function = RPNElement::FUNCTION_LITERAL_ARRAY;
            out.array_literal = const_value.safeGet<Array>();
            out.func_name = "Array literal";
            return true;
        }

        if (const_value.getType() == Field::Types::String)
        {
            out.function = RPNElement::FUNCTION_STRING_LITERAL;
            out.func_name = const_value.safeGet<String>();
            return true;
        }
    }

    return false;
}

void VectorSimilarityCondition::traverseOrderByAST(const ASTPtr & node, RPN & rpn)
{
    if (const auto * expr_list = node->as<ASTExpressionList>())
        if (const auto * order_by_element = expr_list->children.front()->as<ASTOrderByElement>())
            traverseAST(order_by_element->children.front(), rpn);
}

/// Returns true and stores ANNExpr if the query has valid ORDERBY clause
bool VectorSimilarityCondition::matchRPNOrderBy(RPN & rpn, Info & info)
{
    /// ORDER BY clause must have at least 3 expressions
    if (rpn.size() < 3)
        return false;

    auto iter = rpn.begin();
    auto end = rpn.end();

    bool identifier_found = false;

    /// Matches DistanceFunc->[Column]->[ArrayFunc]->ReferenceVector(floats)->[Column]
    if (iter->function != RPNElement::FUNCTION_DISTANCE)
        return false;

    info.distance_function = stringToDistanceFunction(iter->func_name);
    ++iter;

    if (iter->function == RPNElement::FUNCTION_IDENTIFIER)
    {
        identifier_found = true;
        info.column_name = std::move(iter->identifier.value());
        ++iter;
    }

    if (iter->function == RPNElement::FUNCTION_ARRAY)
        ++iter;

    if (iter->function == RPNElement::FUNCTION_LITERAL_ARRAY)
    {
        extractReferenceVectorFromLiteral(info.reference_vector, iter->array_literal);
        ++iter;
    }

    /// further conditions are possible if there is no array, or no identifier is found
    /// the array can be inside a cast function. For other cases, see the loop after this condition
    if (iter != end && iter->function == RPNElement::FUNCTION_CAST)
    {
        ++iter;
        /// Cast should be made to array
        if (!iter->func_name.starts_with("Array"))
            return false;
        ++iter;
        if (iter->function == RPNElement::FUNCTION_LITERAL_ARRAY)
        {
            extractReferenceVectorFromLiteral(info.reference_vector, iter->array_literal);
            ++iter;
        }
        else
            return false;
    }

    while (iter != end)
    {
        if (iter->function == RPNElement::FUNCTION_FLOAT_LITERAL ||
            iter->function == RPNElement::FUNCTION_INT_LITERAL)
            info.reference_vector.emplace_back(getFloatOrIntLiteralOrPanic(iter));
        else if (iter->function == RPNElement::FUNCTION_IDENTIFIER)
        {
            if (identifier_found)
                return false;
            info.column_name = std::move(iter->identifier.value());
            identifier_found = true;
        }
        else
            return false;

        ++iter;
    }

    /// Final checks of correctness
    return identifier_found && !info.reference_vector.empty();
}

/// Returns true and stores Length if we have valid LIMIT clause in query
bool VectorSimilarityCondition::matchRPNLimit(RPNElement & rpn, UInt64 & limit)
{
    if (rpn.function == RPNElement::FUNCTION_INT_LITERAL)
    {
        limit = rpn.int_literal.value();
        return true;
    }

    return false;
}

/// Gets float or int from AST node
float VectorSimilarityCondition::getFloatOrIntLiteralOrPanic(const RPN::iterator& iter)
{
    if (iter->float_literal.has_value())
        return iter->float_literal.value();
    if (iter->int_literal.has_value())
        return static_cast<float>(iter->int_literal.value());
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Wrong parsed AST in buildRPN\n");
}

}
