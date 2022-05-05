#pragma once

#include <Storages/MergeTree/KeyCondition.h>

#include <optional>
#include <vector>

namespace DB
{

namespace ANNCondition
{

/**
    Class ANNCondition, is responsible for recognizing special query types which
    can be speeded up by ANN Indexes. It parses the SQL query and checks
    if it matches ANNIndexes. The recognizing method - alwaysUnknownOrTrue
    returns false if we can speed up the query, and true otherwise.
    It has only one argument, name of the metric with which index was built.
    There are two main patterns of queries being supported

    1) Search query type
    SELECT * FROM * WHERE DistanceFunc(column, target_vector) < floatLiteral [LIMIT count]

    2) OrderBy query type
    SELECT * FROM * WHERE * ORDERBY DistanceFunc(column, target_vector) LIMIT some_length

    target_vector(should have float coordinates) examples:
        tuple(0.1, 0.1, ...., 0.1) or (0.1, 0.1, ...., 0.1)
        [the word tuple is not needed]

    If the query mathces one of these two types, than tha class extracts useful information
    from the query. If the query has both 1 and 2 types, than we can't speed and alwaysUnknownOrTrue
    returns true.

    From matching query it extracts
    * targetVector
    * metricName(DistanceFunction)
    * dimension size if query uses LpDistance
    * distance to compare(ONLY for search types, otherwise you get exception)
    * spaceDimension(which is targetVector's components count)
    * column
    * objects count from LIMIT section(for both queries)
    * settings str, if query has settings section with new 'ann_index_params' value,
        than you can get the new value(empty by default) calling method getSettingsStr
    * queryHasOrderByClause and queryHasWhereClause return true if query mathces the type

    Search query type is also recognized for PREWHERE section
*/

class ANNCondition
{
public:
    ANNCondition(const SelectQueryInfo & query_info,
                    ContextPtr context);

    // flase if query can be speeded up, true otherwise
    bool alwaysUnknownOrTrue(String metric_name) const;

    // returns the distance to compare with for search query
    float getComparisonDistance() const;

    // distance should be calculated regarding to targetVector
    std::vector<float> getTargetVector() const;

    // targetVector dimension size
    size_t getSpaceDim() const;

    // data Column Name in DB
    String getColumnName() const;

    // Distance fucntion name
    String getMetric() const;

    // the P- value if the metric is 'LpDistance'
    float getPForLpDistance() const;

    // true if query match ORDERBY type
    bool queryHasOrderByClause() const;

    // true if query match Search type
    bool queryHasWhereClause() const;

    // length's value from LIMIT section, nullopt if not any
    std::optional<UInt64> getLimitCount() const;

    // value of 'ann_index_params' if have in SETTINGS section, empty string otherwise
    String getSettingsStr() const;

private:
    // Type of the vector to use as a target in the distance function
    using Target = std::vector<float>;

    // Extracted data from valid query
    struct ANNExpression
    {
        Target target;
        float distance = -1.0;
        String metric_name;
        String column_name;
        float p_for_lp_dist = -1.0; // The P parametr for LpDistance
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
            // DistanceFunctions
            FUNCTION_DISTANCE,

            //tuple(0.1, ..., 0.1)
            FUNCTION_TUPLE,

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

    void buildRPN(const SelectQueryInfo & query);

    // Util functions for the traversal of AST, parses AST and builds rpn
    void traverseAST(const ASTPtr & node, RPN & rpn);
    // Return true if we can identify our node type
    bool traverseAtomAST(const ASTPtr & node, RPNElement & out);
    // Checks if the AST stores ConstType expression
    bool tryCastToConstType(const ASTPtr & node, RPNElement & out);
    // Traverses the AST of ORDERBY section
    void traverseOrderByAST(const ASTPtr & node, RPN & rpn);


    // Checks that at least one rpn is matching for index
    // New RPNs for other query types can be added here
    bool matchAllRPNS();

    // Returns true and stores ANNExpr if the query has valid WHERE section
    static bool matchRPNWhere(RPN & rpn, ANNExpression & expr);

    // Returns true and stores ANNExpr if the query has valid ORDERBY section
    static bool matchRPNOrderBy(RPN & rpn, ANNExpression & expr);

    // Returns true and stores Length if we have valid LIMIT clause in query
    static bool matchRPNLimit(RPN & rpn, LimitExpression & expr);

    // Parses SETTINGS section, stores the new value for 'ann_index_params'
    void parseSettings(const ASTPtr & node);


    /* Matches dist function, target vector, coloumn name */
    static bool matchMainParts(RPN::iterator & iter, RPN::iterator & end, ANNExpression & expr, bool & identifier_found);

    // Util methods
    static void panicIfWrongBuiltRPN [[noreturn]] ();
    static String getIdentifierOrPanic(RPN::iterator& iter);
    // Gets float or int from AST node
    static float getFloatOrIntLiteralOrPanic(RPN::iterator& iter);


    // RPN-s for different sections of the query
    RPN rpn_prewhere_clause;
    RPN rpn_where_clause;
    RPN rpn_limit_clause;
    RPN rpn_order_by_clause;

    Block block_with_constants;

    // Data extracted from query, in case query has supported type
    ANNExprOpt ann_expr{std::nullopt};
    LimitExprOpt limit_expr{std::nullopt};
    String ann_index_params; // Empty string if no params

    bool order_by_query_type{false};
    bool where_query_type{false};
    bool has_limit{false};

    // true if we have one of two supported query types
    bool index_is_useful{false};
};

}

}
