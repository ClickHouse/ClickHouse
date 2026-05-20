#pragma once

#include <Parsers/SPARQL/SparqlAST.h>
#include <string>
#include <unordered_map>
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
    };

    int alias_counter = 0;
    std::unordered_map<std::string, VarBinding> bindings;
    std::vector<JoinEntry> joins;

    std::string newAlias();
    void processTriple(const TriplePattern & tp, bool is_optional);
    void processTermBinding(const Term & term, const std::string & alias, const std::string & column);

    std::string buildProjection(const std::vector<std::string> & vars);
    std::string buildFromAndJoins();
    std::string buildWhereClause();
    std::string translateFilterExpr(const FilterExpr & expr);
    std::string translateBranch(const GroupGraphPattern & ggp, const std::vector<std::string> & projection_vars);
    std::string buildUnionQuery(const Union & u, const std::vector<std::string> & projection_vars);

    void reset();
};

}
}
