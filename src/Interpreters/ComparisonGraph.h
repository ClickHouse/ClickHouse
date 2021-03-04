#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <unordered_map>
#include <vector>

namespace DB
{

class ComparisonGraph
{
public:
    ComparisonGraph(const std::vector<ASTPtr> & atomic_formulas);

    /// Works for string and num.
    /// For other -- only eq.
    enum class CompareResult
    {
        LESS,
        LESS_OR_EQUAL,
        EQUAL,
        GREATER_OR_EQUAL,
        GREATER,
        //NOT_EQUAL,
        UNKNOWN,
    };

    // TODO: implement
    CompareResult compare(const ASTPtr & /*left*/, const ASTPtr & /*right*/) const { return CompareResult::UNKNOWN; }

    std::vector<ASTPtr> getEqual(const ASTPtr & ast) const;

    /// Find constants less and greater.
    /// For int and double linear programming can be applied here.
    // TODO: implement
    ASTPtr getMax(const ASTPtr &) const { return nullptr; } // sup
    ASTPtr getMin(const ASTPtr &) const { return nullptr; } // inf

private:
    /// strongly connected component
    struct EqualComponent
    {
        std::vector<ASTPtr> asts;
    };

    /// TODO: move to diff for int and double:
    /// LESS and LESS_OR_EQUAL with +const or 0 --- ok
    ///                        with -const --- not ok
    /// EQUAL is ok only for 0
    struct Edge
    {
        enum Type
        {
            LESS,
            LESS_OR_EQUAL,
            EQUAL,
        };

        Type type;
        EqualComponent to;
    };

    struct Graph
    {
        std::unordered_map<UInt64, size_t> ast_hash_to_component;
        std::vector<EqualComponent> vertexes;
        std::vector<std::vector<Edge>> edges;
    };

    Graph BuildGraphFromAsts(const Graph & asts_graph);

    Graph graph;
};

}
