#pragma once

#include <Parsers/IAST.h>

#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

struct Settings;
class ASTFunction;
class ASTSelectQuery;

/** This class provides functions for optimizing boolean expressions within queries.
  *
  * For simplicity, we call a homogeneous OR-chain any expression having the following structure:
  * expr = x1 OR ... OR expr = xN
  * where `expr` is an arbitrary expression and x1, ..., xN are literals of the same type
  */
class LogicalExpressionsOptimizer final
{
public:
    /// Constructor. Accepts the root of the query DAG.
    LogicalExpressionsOptimizer(ASTSelectQuery * select_query_, const Settings & settings_);

    /** Replace all rather long homogeneous OR-chains expr = x1 OR ... OR expr = xN
      * on the expressions `expr` IN (x1, ..., xN).
      */
    void perform();

    LogicalExpressionsOptimizer(const LogicalExpressionsOptimizer &) = delete;
    LogicalExpressionsOptimizer & operator=(const LogicalExpressionsOptimizer &) = delete;

private:
    /** The OR function with the expression.
    */
    struct OrWithExpression
    {
        OrWithExpression(ASTFunction * or_function_, const IAST::Hash & expression_,
            const std::string & alias_);
        bool operator<(const OrWithExpression & rhs) const;

        ASTFunction * or_function;
        const IAST::Hash expression;
        const std::string alias;
    };

    struct Equalities
    {
        std::vector<ASTFunction *> functions;
        bool is_processed = false;
    };

    using DisjunctiveEqualityChainsMap = std::map<OrWithExpression, Equalities>;
    using DisjunctiveEqualityChain = DisjunctiveEqualityChainsMap::value_type;

private:
    /** Collect information about all the equations in the OR chains (not necessarily homogeneous).
      * This information is grouped by the expression that is on the left side of the equation.
      */
    void collectDisjunctiveEqualityChains();

    /** Check that the set of equalities expr = x1, ..., expr = xN fulfills the following two requirements:
      * 1. It's not too small
      * 2. x1, ... xN have the same type
      */
    bool mayOptimizeDisjunctiveEqualityChain(const DisjunctiveEqualityChain & chain) const;

    /// Insert the IN expression into the OR chain.
    void addInExpression(const DisjunctiveEqualityChain & chain);

    /// Delete the equalities that were replaced by the IN expressions.
    void cleanupOrExpressions();

    /// Delete OR expressions that have only one operand.
    void fixBrokenOrExpressions();

    /// Restore the original column order after optimization.
    void reorderColumns();

private:
    using ParentNodes = std::vector<IAST *>;
    using FunctionParentMap = std::unordered_map<IAST *, ParentNodes>;
    using ColumnToPosition = std::unordered_map<IAST *, size_t>;

private:
    ASTSelectQuery * select_query;
    const Settings & settings;
    /// Information about the OR-chains inside the query.
    DisjunctiveEqualityChainsMap disjunctive_equality_chains_map;
    /// Number of processed OR-chains.
    size_t processed_count = 0;
    /// Parents of OR functions.
    FunctionParentMap or_parent_map;
    /// The position of each column.
    ColumnToPosition column_to_position;
    /// Set of nodes, that was visited.
    std::unordered_set<void *> visited_nodes;
};

}
