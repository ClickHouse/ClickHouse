#pragma once

#include <Storages/MergeTree/KeyCondition.h>
#include "base/types.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

namespace DB
{

namespace Condition
{

class CommonCondition
{
public:
    CommonCondition(const SelectQueryInfo & query_info,
                    ContextPtr context);

    bool alwaysUnknownOrTrue() const;

    float getComparisonDistance() const;

    std::vector<float> getTargetVector() const;

    size_t getSpaceDim() const;

    String getColumnName() const;

    String getMetric() const;

    float getPForLpDistance() const;

    bool queryHasOrderByClause() const;

    bool queryHasWhereClause() const;

    std::optional<UInt64> getLimitLength() const;

    String getSettingsStr() const;

private:
    // Type of the vector to use as a target in the distance function
    using Target = std::vector<float>;

    // Extracted data from the query like WHERE L2Distance(column_name, target) < distance
    struct ANNExpression
    {
        Target target;
        float distance = -1.0;
        String metric_name = "Unknown"; // Metric name, maybe some Enum for all indices
        String column_name = "Unknown"; // Coloumn name stored in IndexGranule
        float p_for_lp_dist = -1.0; // The P parametr for Lp Distance
    };

    struct LimitExpression
    {
        Int64 length;
    };

    using ANNExprOpt = std::optional<ANNExpression>;
    using LimitExprOpt = std::optional<LimitExpression>;
    struct RPNElement
    {
        enum Function
        {
            // l2 dist
            FUNCTION_DISTANCE,

            //tuple(10, 15)
            FUNCTION_TUPLE,

            // Operator <, >
            FUNCTION_COMPARISON,

            // Numeric float value
            FUNCTION_FLOAT_LITERAL,

            // Numeric int value
            FUNCTION_INT_LITERAL,

            // Column identifier
            FUNCTION_IDENTIFIER,

            // Unknown, can be any value
            FUNCTION_UNKNOWN,

            FUNCTION_STRING,

            FUNCTION_LITERAL_TUPLE,

            FUNCTION_ORDER_BY_ELEMENT,
        };

        explicit RPNElement(Function function_ = FUNCTION_UNKNOWN)
        : function(function_), func_name("Unknown"), float_literal(std::nullopt), identifier(std::nullopt) {}

        Function function;
        String func_name;

        std::optional<float> float_literal;
        std::optional<String> identifier;
        std::optional<int64_t> int_literal{std::nullopt};
        std::optional<Tuple> tuple_literal{std::nullopt};

        UInt32 dim{0};
    };

    using RPN = std::vector<RPNElement>;

    void buildRPN(const SelectQueryInfo & query, ContextPtr context);

    // Util functions for the traversal of AST
    void traverseAST(const ASTPtr & node, RPN & rpn);
    // Return true if we can identify our node type
    bool traverseAtomAST(const ASTPtr & node, RPNElement & out);

    // Checks that at least one rpn is matching for index
    // New RPNs for other query types can be added here
    bool matchAllRPNS();

    /* Returns true and stores ANNExpr if the query matches the template:
     * WHERE DistFunc(column_name, tuple(float_1, float_2, ..., float_dim)) < float_literal */
    static bool matchRPNWhere(RPN & rpn, ANNExpression & expr);

    /* Returns true and stores OrderByExpr if the query has valid OrderBy section*/
    static bool matchRPNOrderBy(RPN & rpn, ANNExpression & expr);

    /* Returns true if we have valid limit clause in query*/
    static bool matchRPNLimit(RPN & rpn, LimitExpression & expr);

    /* Getting settings for ann_index_param */
    void parseSettings(const ASTPtr & node);


    /* Matches dist function, target vector, coloumn name */
    static bool matchMainParts(RPN::iterator & iter, RPN::iterator & end, ANNExpression & expr, bool & identifier_found);

    // Util methods
    static void panicIfWrongBuiltRPN [[noreturn]] ();
    static String getIdentifierOrPanic(RPN::iterator& iter);

    static float getFloatOrIntLiteralOrPanic(RPN::iterator& iter);


    // Here we store RPN-s for different types of Queries
    RPN rpn_prewhere_clause;
    RPN rpn_where_clause;
    RPN rpn_limit_clause;
    RPN rpn_order_by_clause;

    Block block_with_constants;

    ANNExprOpt ann_expr{std::nullopt};
    LimitExprOpt limit_expr{std::nullopt};
    String ann_index_params; // Empty string if no params


    bool order_by_query_type{false};
    bool where_query_type{false};
    bool has_limit{false};

    // true if we had extracted ANNExpression from query
    bool index_is_useful{false};

};

}

}
