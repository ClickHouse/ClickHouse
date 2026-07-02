
// Generated from SparqlParser.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "SparqlParser.h"


namespace antlr4_grammars {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by SparqlParser.
 */
class  SparqlParserVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by SparqlParser.
   */
    virtual std::any visitQuery(SparqlParser::QueryContext *context) = 0;

    virtual std::any visitPrologue(SparqlParser::PrologueContext *context) = 0;

    virtual std::any visitBaseDecl(SparqlParser::BaseDeclContext *context) = 0;

    virtual std::any visitPrefixDecl(SparqlParser::PrefixDeclContext *context) = 0;

    virtual std::any visitSelectQuery(SparqlParser::SelectQueryContext *context) = 0;

    virtual std::any visitConstructQuery(SparqlParser::ConstructQueryContext *context) = 0;

    virtual std::any visitDescribeQuery(SparqlParser::DescribeQueryContext *context) = 0;

    virtual std::any visitAskQuery(SparqlParser::AskQueryContext *context) = 0;

    virtual std::any visitDatasetClause(SparqlParser::DatasetClauseContext *context) = 0;

    virtual std::any visitDefaultGraphClause(SparqlParser::DefaultGraphClauseContext *context) = 0;

    virtual std::any visitNamedGraphClause(SparqlParser::NamedGraphClauseContext *context) = 0;

    virtual std::any visitSourceSelector(SparqlParser::SourceSelectorContext *context) = 0;

    virtual std::any visitWhereClause(SparqlParser::WhereClauseContext *context) = 0;

    virtual std::any visitSolutionModifier(SparqlParser::SolutionModifierContext *context) = 0;

    virtual std::any visitLimitOffsetClauses(SparqlParser::LimitOffsetClausesContext *context) = 0;

    virtual std::any visitOrderClause(SparqlParser::OrderClauseContext *context) = 0;

    virtual std::any visitOrderCondition(SparqlParser::OrderConditionContext *context) = 0;

    virtual std::any visitLimitClause(SparqlParser::LimitClauseContext *context) = 0;

    virtual std::any visitOffsetClause(SparqlParser::OffsetClauseContext *context) = 0;

    virtual std::any visitGroupGraphPattern(SparqlParser::GroupGraphPatternContext *context) = 0;

    virtual std::any visitTriplesBlock(SparqlParser::TriplesBlockContext *context) = 0;

    virtual std::any visitGraphPatternNotTriples(SparqlParser::GraphPatternNotTriplesContext *context) = 0;

    virtual std::any visitOptionalGraphPattern(SparqlParser::OptionalGraphPatternContext *context) = 0;

    virtual std::any visitGraphGraphPattern(SparqlParser::GraphGraphPatternContext *context) = 0;

    virtual std::any visitGroupOrUnionGraphPattern(SparqlParser::GroupOrUnionGraphPatternContext *context) = 0;

    virtual std::any visitFilter_(SparqlParser::Filter_Context *context) = 0;

    virtual std::any visitConstraint(SparqlParser::ConstraintContext *context) = 0;

    virtual std::any visitFunctionCall(SparqlParser::FunctionCallContext *context) = 0;

    virtual std::any visitArgList(SparqlParser::ArgListContext *context) = 0;

    virtual std::any visitConstructTemplate(SparqlParser::ConstructTemplateContext *context) = 0;

    virtual std::any visitConstructTriples(SparqlParser::ConstructTriplesContext *context) = 0;

    virtual std::any visitTriplesSameSubject(SparqlParser::TriplesSameSubjectContext *context) = 0;

    virtual std::any visitPropertyListNotEmpty(SparqlParser::PropertyListNotEmptyContext *context) = 0;

    virtual std::any visitPropertyList(SparqlParser::PropertyListContext *context) = 0;

    virtual std::any visitObjectList(SparqlParser::ObjectListContext *context) = 0;

    virtual std::any visitObject_(SparqlParser::Object_Context *context) = 0;

    virtual std::any visitVerb(SparqlParser::VerbContext *context) = 0;

    virtual std::any visitTriplesNode(SparqlParser::TriplesNodeContext *context) = 0;

    virtual std::any visitBlankNodePropertyList(SparqlParser::BlankNodePropertyListContext *context) = 0;

    virtual std::any visitCollection(SparqlParser::CollectionContext *context) = 0;

    virtual std::any visitGraphNode(SparqlParser::GraphNodeContext *context) = 0;

    virtual std::any visitVarOrTerm(SparqlParser::VarOrTermContext *context) = 0;

    virtual std::any visitVarOrIRIref(SparqlParser::VarOrIRIrefContext *context) = 0;

    virtual std::any visitVar_(SparqlParser::Var_Context *context) = 0;

    virtual std::any visitGraphTerm(SparqlParser::GraphTermContext *context) = 0;

    virtual std::any visitExpression(SparqlParser::ExpressionContext *context) = 0;

    virtual std::any visitConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext *context) = 0;

    virtual std::any visitConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext *context) = 0;

    virtual std::any visitValueLogical(SparqlParser::ValueLogicalContext *context) = 0;

    virtual std::any visitRelationalExpression(SparqlParser::RelationalExpressionContext *context) = 0;

    virtual std::any visitNumericExpression(SparqlParser::NumericExpressionContext *context) = 0;

    virtual std::any visitAdditiveExpression(SparqlParser::AdditiveExpressionContext *context) = 0;

    virtual std::any visitMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext *context) = 0;

    virtual std::any visitUnaryExpression(SparqlParser::UnaryExpressionContext *context) = 0;

    virtual std::any visitPrimaryExpression(SparqlParser::PrimaryExpressionContext *context) = 0;

    virtual std::any visitBrackettedExpression(SparqlParser::BrackettedExpressionContext *context) = 0;

    virtual std::any visitBuiltInCall(SparqlParser::BuiltInCallContext *context) = 0;

    virtual std::any visitRegexExpression(SparqlParser::RegexExpressionContext *context) = 0;

    virtual std::any visitIriRefOrFunction(SparqlParser::IriRefOrFunctionContext *context) = 0;

    virtual std::any visitRdfLiteral(SparqlParser::RdfLiteralContext *context) = 0;

    virtual std::any visitNumericLiteral(SparqlParser::NumericLiteralContext *context) = 0;

    virtual std::any visitNumericLiteralUnsigned(SparqlParser::NumericLiteralUnsignedContext *context) = 0;

    virtual std::any visitNumericLiteralPositive(SparqlParser::NumericLiteralPositiveContext *context) = 0;

    virtual std::any visitNumericLiteralNegative(SparqlParser::NumericLiteralNegativeContext *context) = 0;

    virtual std::any visitBooleanLiteral(SparqlParser::BooleanLiteralContext *context) = 0;

    virtual std::any visitString_(SparqlParser::String_Context *context) = 0;

    virtual std::any visitIriRef(SparqlParser::IriRefContext *context) = 0;

    virtual std::any visitPrefixedName(SparqlParser::PrefixedNameContext *context) = 0;

    virtual std::any visitBlankNode(SparqlParser::BlankNodeContext *context) = 0;


};

}  // namespace antlr4_grammars
