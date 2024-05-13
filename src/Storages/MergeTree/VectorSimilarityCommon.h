#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include "base/types.h"

#include <optional>
#include <vector>

namespace DB
{

static constexpr auto DISTANCE_FUNCTION_L2 = "L2Distance";
static constexpr auto DISTANCE_FUNCTION_COSINE = "cosineDistance";

/// Approximate nearest neighbour (vector similarity) queries have a similar structure:
/// - reference vector from which all distances are calculated
/// - metric, e.g. L2Distance
/// - name of column with embeddings
/// - maximum number of returned elements (LIMIT)
struct VectorSimilarityInfo
{
    using Embedding = std::vector<float>;
    Embedding reference_vector;

    enum class Metric : uint8_t
    {
        Unknown,
        L2
    };
    Metric metric;

    String column_name;
    UInt64 limit;

    float distance = -1.0;
};


/// Class VectorSimilarityCondition, is responsible for recognizing if the query can utilize vector similarity indexes.
/// Method alwaysUnknownOrTrue returns false if we can speed up the query, and true otherwise. It has
/// only one argument, the name of the metric with which index was built. This pattern of queries is supported:
///
///   SELECT *
///   FROM *
///   ORDER BY DistanceFunc(column, reference_vector)
///   LIMIT count
///
/// Queries without LIMIT count are not supported.
/// The reference_vector should have float coordinates, e.g. (0.2, 0.1, .., 0.5)
///
/// If the query matches this template, then this class extracts the main information needed for vector similarity indexes from the query.
///
/// From matching query it extracts
/// - referenceVector
/// - metricName(DistanceFunction)
/// - the dimension of the reference vector
/// - column
/// - objects count from LIMIT clause(for both queries)
class VectorSimilarityCondition
{
public:
    VectorSimilarityCondition(const SelectQueryInfo & query_info, ContextPtr context);

    /// Returns false if query can be speeded up by a vector similarity index, true otherwise.
    bool alwaysUnknownOrTrue(String metric) const;

    /// Distance should be calculated regarding to referenceVector
    std::vector<float> getReferenceVector() const;

    /// Reference vector's dimension count
    size_t getDimensions() const;

    String getColumnName() const;

    VectorSimilarityInfo::Metric getMetricType() const;

    UInt64 getIndexGranularity() const { return index_granularity; }

    /// Length's value from LIMIT clause
    UInt64 getLimit() const;

private:
    struct RPNElement
    {
        enum Function
        {
            /// DistanceFunctions
            FUNCTION_DISTANCE,

            //array(0.1, ..., 0.1)
            FUNCTION_ARRAY,

            /// Numeric float value
            FUNCTION_FLOAT_LITERAL,

            /// Numeric int value
            FUNCTION_INT_LITERAL,

            /// Column identifier
            FUNCTION_IDENTIFIER,

            /// Unknown, can be any value
            FUNCTION_UNKNOWN,

            /// [0.1, ...., 0.1] vector without word 'array'
            FUNCTION_LITERAL_ARRAY,

            /// if client parameters are used, cast will always be in the query
            FUNCTION_CAST,

            /// name of type in cast function
            FUNCTION_STRING_LITERAL,
        };

        explicit RPNElement(Function function_ = FUNCTION_UNKNOWN)
            : function(function_)
            , func_name("Unknown")
            , float_literal(std::nullopt)
            , identifier(std::nullopt)
        {}

        Function function;
        String func_name;

        std::optional<float> float_literal;
        std::optional<String> identifier;
        std::optional<int64_t> int_literal;

        std::optional<Array> array_literal;

        UInt32 dim = 0;
    };

    using RPN = std::vector<RPNElement>;

    bool checkQueryStructure(const SelectQueryInfo & query);

    /// Util functions for the traversal of AST, parses AST and builds rpn
    void traverseAST(const ASTPtr & node, RPN & rpn);
    /// Return true if we can identify our node type
    bool traverseAtomAST(const ASTPtr & node, RPNElement & out);
    /// Checks if the AST stores ConstType expression
    bool tryCastToConstType(const ASTPtr & node, RPNElement & out);
    /// Traverses the AST of ORDERBY section
    void traverseOrderByAST(const ASTPtr & node, RPN & rpn);

    /// Returns true and stores ANNExpr if the query has valid ORDERBY section
    static bool matchRPNOrderBy(RPN & rpn, VectorSimilarityInfo & vector_similarity_info);

    /// Returns true and stores Length if we have valid LIMIT clause in query
    static bool matchRPNLimit(RPNElement & rpn, UInt64 & limit);

    /// Gets float or int from AST node
    static float getFloatOrIntLiteralOrPanic(const RPN::iterator& iter);

    Block block_with_constants;

    /// Set if the query is supported
    std::optional<VectorSimilarityInfo> query_information;

    // Get from settings ANNIndex parameters
    const UInt64 index_granularity;

    /// only queries with a lower limit can be considered to avoid memory overflow
    const UInt64 max_limit_for_ann_queries;

    bool index_is_useful = false;
};


/// Common interface of vector similarity indexes
class IMergeTreeIndexConditionVectorSimilarity : public IMergeTreeIndexCondition
{
public:
    /// Returns vector of indexes of ranges in granule which are useful for query.
    virtual std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const = 0;
};

}
