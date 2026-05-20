#pragma once

#include <memory>
#include <string>
#include <vector>

namespace DB
{
namespace SPARQL
{

struct Term
{
    enum Kind { Variable, IRI, Literal };

    Kind kind;
    std::string value;
    std::string datatype;
    std::string lang;
};

struct TriplePattern
{
    Term subject;
    Term predicate;
    Term object;
};

struct FilterExpr;
using FilterExprPtr = std::unique_ptr<FilterExpr>;

struct FilterExpr
{
    enum Op
    {
        And, Or, Not,
        Eq, Ne, Lt, Gt, Le, Ge,
        Plus, Minus, Mul, Div,
        VarRef, LiteralVal,
        FnStr, FnLang, FnDatatype, FnBound,
        FnIsIRI, FnIsBlank, FnIsLiteral,
        FnSameTerm, FnLangMatches, FnRegex,
        FnStrlen
    };

    Op op;
    std::string value;
    std::vector<FilterExprPtr> children;
};

struct GroupGraphPattern;
using GroupGraphPatternPtr = std::unique_ptr<GroupGraphPattern>;

struct Optional
{
    GroupGraphPatternPtr pattern;
};

struct Union
{
    std::vector<GroupGraphPatternPtr> alternatives;
};

struct GroupGraphPattern
{
    std::vector<TriplePattern> triples;
    std::vector<Optional> optionals;
    std::vector<Union> unions;
    std::vector<FilterExprPtr> filters;
};

struct PrefixDecl
{
    std::string prefix;
    std::string uri;
};

struct SelectQuery
{
    std::vector<std::string> projection;
    GroupGraphPatternPtr where_clause;
    std::vector<PrefixDecl> prefixes;
};

}
}
