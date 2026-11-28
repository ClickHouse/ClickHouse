#pragma once

#include <Parsers/SPARQL/SparqlAST.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wdocumentation-deprecated-sync"
#pragma clang diagnostic ignored "-Wdocumentation-html"
#pragma clang diagnostic ignored "-Wextra-semi"
#pragma clang diagnostic ignored "-Winconsistent-missing-destructor-override"
#pragma clang diagnostic ignored "-Wshadow-field"
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#pragma clang diagnostic ignored "-Wsuggest-destructor-override"
#include <antlr4_grammars/SparqlParser.h>
#pragma clang diagnostic pop

namespace DB
{
namespace SPARQL
{

class CstToAstBuilder
{
public:
    std::unique_ptr<SelectQuery> build(antlr4_grammars::SparqlParser::QueryContext * query_ctx);

private:
    std::vector<PrefixDecl> prefixes_;

    void collectPrefixes(antlr4_grammars::SparqlParser::PrologueContext * ctx);
    std::string expandPrefixedName(const std::string & text) const;

    std::unique_ptr<SelectQuery> buildSelectQuery(antlr4_grammars::SparqlParser::SelectQueryContext * ctx);
    GroupGraphPatternPtr buildGroupGraphPattern(antlr4_grammars::SparqlParser::GroupGraphPatternContext * ctx);
    std::vector<TriplePattern> buildTriplePatterns(antlr4_grammars::SparqlParser::TriplesSameSubjectContext * ctx);
    Term buildTerm(antlr4_grammars::SparqlParser::VarOrTermContext * ctx);
    Term buildTermFromVar(antlr4_grammars::SparqlParser::Var_Context * ctx);
    Term buildTermFromIRI(antlr4_grammars::SparqlParser::IriRefContext * ctx);
    Term buildTermFromLiteral(antlr4_grammars::SparqlParser::RdfLiteralContext * ctx);
    Term buildTermFromNumeric(antlr4_grammars::SparqlParser::NumericLiteralContext * ctx);

    FilterExprPtr buildFilterExpr(antlr4_grammars::SparqlParser::ExpressionContext * ctx);
    FilterExprPtr buildConditionalOr(antlr4_grammars::SparqlParser::ConditionalOrExpressionContext * ctx);
    FilterExprPtr buildConditionalAnd(antlr4_grammars::SparqlParser::ConditionalAndExpressionContext * ctx);
    FilterExprPtr buildRelational(antlr4_grammars::SparqlParser::RelationalExpressionContext * ctx);
    FilterExprPtr buildAdditive(antlr4_grammars::SparqlParser::AdditiveExpressionContext * ctx);
    FilterExprPtr buildMultiplicative(antlr4_grammars::SparqlParser::MultiplicativeExpressionContext * ctx);
    FilterExprPtr buildUnary(antlr4_grammars::SparqlParser::UnaryExpressionContext * ctx);
    FilterExprPtr buildPrimary(antlr4_grammars::SparqlParser::PrimaryExpressionContext * ctx);
    FilterExprPtr buildBuiltInCall(antlr4_grammars::SparqlParser::BuiltInCallContext * ctx);
};

}
}
