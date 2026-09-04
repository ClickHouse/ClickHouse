#pragma once

#include <Parsers/SPARQL/SparqlAST.h>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{
namespace SPARQL
{

class SparqlToSqlTranslator
{
public:
    std::string translate(const SelectQuery & query);

private:
    struct VarBinding
    {
        std::string alias;
        std::string column;
    };

    struct JoinEntry
    {
        std::string alias;
        bool is_left_join = false;
        std::vector<std::string> on_conditions;
        std::vector<std::string> const_conditions;
        std::vector<std::string> filter_conditions;
    };

    int alias_counter = 0;
    std::unordered_map<std::string, VarBinding> bindings;
    std::vector<JoinEntry> joins;

    std::string newAlias();
    void processTriple(const TriplePattern & tp, bool is_optional);
    void processTermBinding(const Term & term, const std::string & alias, const std::string & column);

    std::string buildProjection(const std::vector<std::string> & vars, bool distinct = false);
    std::string buildFromAndJoins();
    std::string buildPrewhereClause();
    std::string buildWhereClause();
    std::string buildSolutionModifiers(const SelectQuery & query);
    std::string translateFilterExpr(const FilterExpr & expr);
    std::string translateBranch(const GroupGraphPattern & ggp, const std::vector<std::string> & projection_vars, bool distinct = false);
    std::string buildUnionQuery(const Union & u, const std::vector<std::string> & projection_vars);

    static int tripleSelectivityScore(const TriplePattern & tp);
    static void collectFilterVars(const FilterExpr & expr, std::unordered_set<std::string> & vars);

    void reset();
};

}
}
