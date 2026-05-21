
// Generated from SparqlParser.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "SparqlParserVisitor.h"


namespace antlr4_grammars {

/**
 * This class provides an empty implementation of SparqlParserVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the available methods.
 */
class  SparqlParserBaseVisitor : public SparqlParserVisitor {
public:

  virtual std::any visitQuery(SparqlParser::QueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrologue(SparqlParser::PrologueContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBaseDecl(SparqlParser::BaseDeclContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrefixDecl(SparqlParser::PrefixDeclContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSelectQuery(SparqlParser::SelectQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstructQuery(SparqlParser::ConstructQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDescribeQuery(SparqlParser::DescribeQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAskQuery(SparqlParser::AskQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDatasetClause(SparqlParser::DatasetClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDefaultGraphClause(SparqlParser::DefaultGraphClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedGraphClause(SparqlParser::NamedGraphClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSourceSelector(SparqlParser::SourceSelectorContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWhereClause(SparqlParser::WhereClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSolutionModifier(SparqlParser::SolutionModifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLimitOffsetClauses(SparqlParser::LimitOffsetClausesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOrderClause(SparqlParser::OrderClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOrderCondition(SparqlParser::OrderConditionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLimitClause(SparqlParser::LimitClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOffsetClause(SparqlParser::OffsetClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupGraphPattern(SparqlParser::GroupGraphPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriplesBlock(SparqlParser::TriplesBlockContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphPatternNotTriples(SparqlParser::GraphPatternNotTriplesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOptionalGraphPattern(SparqlParser::OptionalGraphPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphGraphPattern(SparqlParser::GraphGraphPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupOrUnionGraphPattern(SparqlParser::GroupOrUnionGraphPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFilter_(SparqlParser::Filter_Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraint(SparqlParser::ConstraintContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunctionCall(SparqlParser::FunctionCallContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitArgList(SparqlParser::ArgListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstructTemplate(SparqlParser::ConstructTemplateContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstructTriples(SparqlParser::ConstructTriplesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriplesSameSubject(SparqlParser::TriplesSameSubjectContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyListNotEmpty(SparqlParser::PropertyListNotEmptyContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyList(SparqlParser::PropertyListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitObjectList(SparqlParser::ObjectListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitObject_(SparqlParser::Object_Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVerb(SparqlParser::VerbContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriplesNode(SparqlParser::TriplesNodeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBlankNodePropertyList(SparqlParser::BlankNodePropertyListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCollection(SparqlParser::CollectionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphNode(SparqlParser::GraphNodeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVarOrTerm(SparqlParser::VarOrTermContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVarOrIRIref(SparqlParser::VarOrIRIrefContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVar_(SparqlParser::Var_Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGraphTerm(SparqlParser::GraphTermContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression(SparqlParser::ExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitValueLogical(SparqlParser::ValueLogicalContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelationalExpression(SparqlParser::RelationalExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericExpression(SparqlParser::NumericExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAdditiveExpression(SparqlParser::AdditiveExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnaryExpression(SparqlParser::UnaryExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrimaryExpression(SparqlParser::PrimaryExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBrackettedExpression(SparqlParser::BrackettedExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBuiltInCall(SparqlParser::BuiltInCallContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRegexExpression(SparqlParser::RegexExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIriRefOrFunction(SparqlParser::IriRefOrFunctionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRdfLiteral(SparqlParser::RdfLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericLiteral(SparqlParser::NumericLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericLiteralUnsigned(SparqlParser::NumericLiteralUnsignedContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericLiteralPositive(SparqlParser::NumericLiteralPositiveContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericLiteralNegative(SparqlParser::NumericLiteralNegativeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBooleanLiteral(SparqlParser::BooleanLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitString_(SparqlParser::String_Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIriRef(SparqlParser::IriRefContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrefixedName(SparqlParser::PrefixedNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBlankNode(SparqlParser::BlankNodeContext *ctx) override {
    return visitChildren(ctx);
  }


};

}  // namespace antlr4_grammars
