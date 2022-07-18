#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include "base/types.h"

#include <optional>
#include <vector>

namespace DB
{

namespace ApproximateNearestNeighbour
{

/**
 * Queries for Approximate Nearest Neighbour Search
 * have similar structure:
 *    1) target vector from which all distances are calculated
 *    2) metric name (e.g L2Distance, LpDistance, etc.)
 *    3) name of column with embeddings
 *    4) type of query
 *    5) Number of elements, that should be taken (limit)
 *
 * And two optional parameters:
 *    1) p for LpDistance function
 *    2) distance to compare with (only for where queries)
 */
struct ANNQueryInformation
{
    using Embedding = std::vector<float>;

    // Extracted data from valid query
    Embedding target;
    enum class Metric
    {
        Unknown,
        L2,
        Lp
    } metric;
    String column_name;
    UInt64 limit;

    enum class Type
    {
        OrderBy,
        Where
    } query_type;

    float p_for_lp_dist = -1.0;
    float distance = -1.0;
};

/**
    Class ANNCondition, is responsible for recognizing special query types which
    can be speeded up by ANN Indexes. It parses the SQL query and checks
    if it matches ANNIndexes. The recognizing method - alwaysUnknownOrTrue
    returns false if we can speed up the query, and true otherwise.
    It has only one argument, name of the metric with which index was built.
    There are two main patterns of queries being supported

    1) Search query type
    SELECT * FROM * WHERE DistanceFunc(column, target_vector) < floatLiteral LIMIT count

    2) OrderBy query type
    SELECT * FROM * WHERE * ORDERBY DistanceFunc(column, target_vector) LIMIT count

    *Query without LIMIT count is not supported*

    target_vector(should have float coordinates) examples:
        tuple(0.1, 0.1, ...., 0.1) or (0.1, 0.1, ...., 0.1)
        [the word tuple is not needed]

    If the query matches one of these two types, than the class extracts useful information
    from the query. If the query has both 1 and 2 types, than we can't speed and alwaysUnknownOrTrue
    returns true.

    From matching query it extracts
    * targetVector
    * metricName(DistanceFunction)
    * dimension size if query uses LpDistance
    * distance to compare(ONLY for search types, otherwise you get exception)
    * spaceDimension(which is targetVector's components count)
    * column
    * objects count from LIMIT clause(for both queries)
    * settings str, if query has settings section with new 'ann_index_select_query_params' value,
        than you can get the new value(empty by default) calling method getSettingsStr
    * queryHasOrderByClause and queryHasWhereClause return true if query matches the type

    Search query type is also recognized for PREWHERE clause
*/

class ANNCondition
{
public:
    ANNCondition(const SelectQueryInfo & query_info,
                    ContextPtr context);

    // false if query can be speeded up, true otherwise
    bool alwaysUnknownOrTrue(String metric_name) const;

    // returns the distance to compare with for search query
    float getComparisonDistanceForWhereQuery() const;

    // distance should be calculated regarding to targetVector
    std::vector<float> getTargetVector() const;

    // targetVector dimension size
    size_t getNumOfDimensions() const;

    String getColumnName() const;

    ANNQueryInformation::Metric getMetricType() const;

    // the P- value if the metric is 'LpDistance'
    float getPValueForLpDistance() const;

    ANNQueryInformation::Type getQueryType() const;

    UInt64 getIndexGranularity() const { return index_granularity; }

    // length's value from LIMIT clause
    UInt64 getLimit() const;

    // value of 'ann_index_select_query_params' if have in SETTINGS clause, empty string otherwise
    String getParamsStr() const { return ann_index_select_query_params; }

private:

    struct RPNElement
    {
        enum Function
        {
            // DistanceFunctions
            FUNCTION_DISTANCE,

            //tuple(0.1, ..., 0.1)
            FUNCTION_TUPLE,

            //array(0.1, ..., 0.1)
            FUNCTION_ARRAY,

            // Operators <, >, <=, >=
            FUNCTION_COMPARISON,

            // Numeric float value
            FUNCTION_FLOAT_LITERAL,

            // Numeric int value
            FUNCTION_INT_LITERAL,

            // Column identifier
            FUNCTION_IDENTIFIER,

            // Unknown, can be any value
            FUNCTION_UNKNOWN,

            // (0.1, ...., 0.1) vector without word 'tuple'
            FUNCTION_LITERAL_TUPLE,

            // [0.1, ...., 0.1] vector without word 'array'
            FUNCTION_LITERAL_ARRAY,
        };

        explicit RPNElement(Function function_ = FUNCTION_UNKNOWN)
        : function(function_), func_name("Unknown"), float_literal(std::nullopt), identifier(std::nullopt) {}

        Function function;
        String func_name;

        std::optional<float> float_literal;
        std::optional<String> identifier;
        std::optional<int64_t> int_literal;

        std::optional<Tuple> tuple_literal;
        std::optional<Array> array_literal;

        UInt32 dim = 0;
    };

    using RPN = std::vector<RPNElement>;

    bool checkQueryStructure(const SelectQueryInfo & query);

    // Util functions for the traversal of AST, parses AST and builds rpn
    void traverseAST(const ASTPtr & node, RPN & rpn);
    // Return true if we can identify our node type
    bool traverseAtomAST(const ASTPtr & node, RPNElement & out);
    // Checks if the AST stores ConstType expression
    bool tryCastToConstType(const ASTPtr & node, RPNElement & out);
    // Traverses the AST of ORDERBY section
    void traverseOrderByAST(const ASTPtr & node, RPN & rpn);

    // Returns true and stores ANNExpr if the query has valid WHERE section
    static bool matchRPNWhere(RPN & rpn, ANNQueryInformation & expr);

    // Returns true and stores ANNExpr if the query has valid ORDERBY section
    static bool matchRPNOrderBy(RPN & rpn, ANNQueryInformation & expr);

    // Returns true and stores Length if we have valid LIMIT clause in query
    static bool matchRPNLimit(RPNElement & rpn, UInt64 & limit);

    /* Matches dist function, target vector, column name */
    static bool matchMainParts(RPN::iterator & iter, const RPN::iterator & end, ANNQueryInformation & expr);

    // Gets float or int from AST node
    static float getFloatOrIntLiteralOrPanic(const RPN::iterator& iter);

    Block block_with_constants;

    // true if we have one of two supported query types
    std::optional<ANNQueryInformation> query_information;

    // Get from settings ANNIndex parameters
    String ann_index_select_query_params;
    UInt64 index_granularity;
    bool index_is_useful = false;
};

// condition interface for Ann indexes. Returns vector of indexes of ranges in granule which are useful for query.
class IMergeTreeIndexConditionAnn : public IMergeTreeIndexCondition
{
public:
    virtual std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const = 0;
};

}

}
