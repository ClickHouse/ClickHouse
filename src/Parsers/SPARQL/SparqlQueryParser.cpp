#include <Parsers/SPARQL/SparqlQueryParser.h>
#include <Parsers/SPARQL/CstToAstBuilder.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wdocumentation-deprecated-sync"
#pragma clang diagnostic ignored "-Wdocumentation-html"
#pragma clang diagnostic ignored "-Wextra-semi"
#pragma clang diagnostic ignored "-Winconsistent-missing-destructor-override"
#pragma clang diagnostic ignored "-Wshadow-field"
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#pragma clang diagnostic ignored "-Wsuggest-destructor-override"
#include <antlr4_grammars/SparqlLexer.h>
#include <antlr4_grammars/SparqlParser.h>
#include <antlr4-runtime.h>
#pragma clang diagnostic pop

#include <stdexcept>

namespace DB
{
namespace SPARQL
{

std::unique_ptr<SelectQuery> SparqlQueryParser::parse(const std::string & sparql)
{
    antlr4::ANTLRInputStream input(sparql);
    antlr4_grammars::SparqlLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    antlr4_grammars::SparqlParser parser(&tokens);

    auto * query_ctx = parser.query();
    if (parser.getNumberOfSyntaxErrors() > 0)
        throw std::runtime_error("SPARQL syntax error in query");

    if (!query_ctx->selectQuery())
        throw std::runtime_error("Only SELECT queries are supported");

    CstToAstBuilder builder;
    return builder.build(query_ctx);
}

}
}
