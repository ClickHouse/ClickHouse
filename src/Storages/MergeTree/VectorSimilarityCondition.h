#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include "base/types.h"

#include <optional>
#include <vector>

namespace DB
{

/// Class VectorSimilarityCondition is responsible for recognizing if the query
/// can utilize vector similarity indexes.
///
/// Method alwaysUnknownOrTrue returns false if we can speed up the query, and true otherwise.
/// It has only one argument, the name of the distance function with which index was built.
/// Only queries with ORDER BY DistanceFunc and LIMIT, i.e.:
///
///   SELECT * FROM * ... ORDER BY DistanceFunc(column, reference_vector) LIMIT count
///
/// reference_vector should have float coordinates, e.g. [0.2, 0.1, .., 0.5]
class VectorSimilarityCondition
{
public:
    VectorSimilarityCondition(const SelectQueryInfo & query_info, ContextPtr context);

    /// vector similarity queries have a similar structure:
    /// - reference vector from which all distances are calculated
    /// - distance function, e.g L2Distance
    /// - name of column with embeddings
    /// - type of query
    /// - maximum number of returned elements (LIMIT)
    ///
    /// And one optional parameter:
    /// - distance to compare with (only for where queries)
    ///
    /// This struct holds all these components.
    struct Info
    {
        enum class DistanceFunction : uint8_t
        {
            Unknown,
            L2,
            Cosine
        };

        std::vector<Float64> reference_vector;
        DistanceFunction distance_function;
        String column_name;
        UInt64 limit;
        float distance = -1.0;
    };

    /// Returns false if query can be speeded up by an ANN index, true otherwise.
    bool alwaysUnknownOrTrue(const String & distance_function) const;

    std::vector<Float64> getReferenceVector() const;
    size_t getDimensions() const;
    String getColumnName() const;
    Info::DistanceFunction getDistanceFunction() const;
    UInt64 getLimit() const;

private:
    struct RPNElement
    {
        enum Function
        {
            /// DistanceFunctions
            FUNCTION_DISTANCE,

            /// array(0.1, ..., 0.1)
            FUNCTION_ARRAY,

            /// Operators <, >, <=, >=
            FUNCTION_COMPARISON,

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
        {}

        Function function;
        String func_name = "Unknown";

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
    static bool matchRPNOrderBy(RPN & rpn, Info & info);

    /// Returns true and stores Length if we have valid LIMIT clause in query
    static bool matchRPNLimit(RPNElement & rpn, UInt64 & limit);

    /// Gets float or int from AST node
    static float getFloatOrIntLiteralOrPanic(const RPN::iterator& iter);

    Block block_with_constants;

    /// true if we have one of two supported query types
    std::optional<Info> query_information;

    /// only queries with a lower limit can be considered to avoid memory overflow
    const UInt64 max_limit_for_ann_queries;

    bool index_is_useful = false;
};

}
