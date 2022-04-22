#pragma once

#include <Storages/MergeTree/KeyCondition.h>

#include <cstddef>
#include <memory>
#include <optional>
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

    using ANNExpressionOpt = std::optional<ANNExpression>;
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

            // Column identifier
            FUNCTION_IDENTIFIER,

            // Unknown, can be any value
            FUNCTION_UNKNOWN,
        };

        explicit RPNElement(Function function_ = FUNCTION_UNKNOWN)
        : function(function_), func_name("Unknown"), float_literal(std::nullopt), identifier(std::nullopt) {}

        Function function;
        String func_name;

        std::optional<float> float_literal;
        std::optional<String> identifier;

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

    /* Returns true and stores ANNExpr if the querry matches the template:
     * WHERE DistFunc(column_name, tuple(float_1, float_2, ..., float_dim)) < float_literal */
    static bool matchRPNWhere(RPN & rpn, ANNExpression & expr);

    /* Matches dist function, target vector, coloumn name */
    static bool matchMainParts(RPN::iterator & iter, RPN::iterator & end, ANNExpression & expr, bool & identifier_found);

    // Util methods
    static void panicIfWrongBuiltRPN [[noreturn]] ();
    static String getIdentifierOrPanic(RPN::iterator& iter);

    static float getFloatLiteralOrPanic(RPN::iterator& iter);


    // Here we store RPN-s for different types of Queries
    RPN rpn_prewhere_clause;
    RPN rpn_where_clause;

    Block block_with_constants;

    ANNExpressionOpt expression{std::nullopt};

    // true if we had extracted ANNExpression from query
    bool index_is_useful{false};

};

}

}
