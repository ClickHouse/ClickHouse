
// Generated from SparqlParser.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "SparqlParser.h"


namespace antlr4_grammars {

/**
 * This interface defines an abstract listener for a parse tree produced by SparqlParser.
 */
class  SparqlParserListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterQuery(SparqlParser::QueryContext *ctx) = 0;
  virtual void exitQuery(SparqlParser::QueryContext *ctx) = 0;

  virtual void enterPrologue(SparqlParser::PrologueContext *ctx) = 0;
  virtual void exitPrologue(SparqlParser::PrologueContext *ctx) = 0;

  virtual void enterBaseDecl(SparqlParser::BaseDeclContext *ctx) = 0;
  virtual void exitBaseDecl(SparqlParser::BaseDeclContext *ctx) = 0;

  virtual void enterPrefixDecl(SparqlParser::PrefixDeclContext *ctx) = 0;
  virtual void exitPrefixDecl(SparqlParser::PrefixDeclContext *ctx) = 0;

  virtual void enterSelectQuery(SparqlParser::SelectQueryContext *ctx) = 0;
  virtual void exitSelectQuery(SparqlParser::SelectQueryContext *ctx) = 0;

  virtual void enterConstructQuery(SparqlParser::ConstructQueryContext *ctx) = 0;
  virtual void exitConstructQuery(SparqlParser::ConstructQueryContext *ctx) = 0;

  virtual void enterDescribeQuery(SparqlParser::DescribeQueryContext *ctx) = 0;
  virtual void exitDescribeQuery(SparqlParser::DescribeQueryContext *ctx) = 0;

  virtual void enterAskQuery(SparqlParser::AskQueryContext *ctx) = 0;
  virtual void exitAskQuery(SparqlParser::AskQueryContext *ctx) = 0;

  virtual void enterDatasetClause(SparqlParser::DatasetClauseContext *ctx) = 0;
  virtual void exitDatasetClause(SparqlParser::DatasetClauseContext *ctx) = 0;

  virtual void enterDefaultGraphClause(SparqlParser::DefaultGraphClauseContext *ctx) = 0;
  virtual void exitDefaultGraphClause(SparqlParser::DefaultGraphClauseContext *ctx) = 0;

  virtual void enterNamedGraphClause(SparqlParser::NamedGraphClauseContext *ctx) = 0;
  virtual void exitNamedGraphClause(SparqlParser::NamedGraphClauseContext *ctx) = 0;

  virtual void enterSourceSelector(SparqlParser::SourceSelectorContext *ctx) = 0;
  virtual void exitSourceSelector(SparqlParser::SourceSelectorContext *ctx) = 0;

  virtual void enterWhereClause(SparqlParser::WhereClauseContext *ctx) = 0;
  virtual void exitWhereClause(SparqlParser::WhereClauseContext *ctx) = 0;

  virtual void enterSolutionModifier(SparqlParser::SolutionModifierContext *ctx) = 0;
  virtual void exitSolutionModifier(SparqlParser::SolutionModifierContext *ctx) = 0;

  virtual void enterLimitOffsetClauses(SparqlParser::LimitOffsetClausesContext *ctx) = 0;
  virtual void exitLimitOffsetClauses(SparqlParser::LimitOffsetClausesContext *ctx) = 0;

  virtual void enterOrderClause(SparqlParser::OrderClauseContext *ctx) = 0;
  virtual void exitOrderClause(SparqlParser::OrderClauseContext *ctx) = 0;

  virtual void enterOrderCondition(SparqlParser::OrderConditionContext *ctx) = 0;
  virtual void exitOrderCondition(SparqlParser::OrderConditionContext *ctx) = 0;

  virtual void enterLimitClause(SparqlParser::LimitClauseContext *ctx) = 0;
  virtual void exitLimitClause(SparqlParser::LimitClauseContext *ctx) = 0;

  virtual void enterOffsetClause(SparqlParser::OffsetClauseContext *ctx) = 0;
  virtual void exitOffsetClause(SparqlParser::OffsetClauseContext *ctx) = 0;

  virtual void enterGroupGraphPattern(SparqlParser::GroupGraphPatternContext *ctx) = 0;
  virtual void exitGroupGraphPattern(SparqlParser::GroupGraphPatternContext *ctx) = 0;

  virtual void enterTriplesBlock(SparqlParser::TriplesBlockContext *ctx) = 0;
  virtual void exitTriplesBlock(SparqlParser::TriplesBlockContext *ctx) = 0;

  virtual void enterGraphPatternNotTriples(SparqlParser::GraphPatternNotTriplesContext *ctx) = 0;
  virtual void exitGraphPatternNotTriples(SparqlParser::GraphPatternNotTriplesContext *ctx) = 0;

  virtual void enterOptionalGraphPattern(SparqlParser::OptionalGraphPatternContext *ctx) = 0;
  virtual void exitOptionalGraphPattern(SparqlParser::OptionalGraphPatternContext *ctx) = 0;

  virtual void enterGraphGraphPattern(SparqlParser::GraphGraphPatternContext *ctx) = 0;
  virtual void exitGraphGraphPattern(SparqlParser::GraphGraphPatternContext *ctx) = 0;

  virtual void enterGroupOrUnionGraphPattern(SparqlParser::GroupOrUnionGraphPatternContext *ctx) = 0;
  virtual void exitGroupOrUnionGraphPattern(SparqlParser::GroupOrUnionGraphPatternContext *ctx) = 0;

  virtual void enterFilter_(SparqlParser::Filter_Context *ctx) = 0;
  virtual void exitFilter_(SparqlParser::Filter_Context *ctx) = 0;

  virtual void enterConstraint(SparqlParser::ConstraintContext *ctx) = 0;
  virtual void exitConstraint(SparqlParser::ConstraintContext *ctx) = 0;

  virtual void enterFunctionCall(SparqlParser::FunctionCallContext *ctx) = 0;
  virtual void exitFunctionCall(SparqlParser::FunctionCallContext *ctx) = 0;

  virtual void enterArgList(SparqlParser::ArgListContext *ctx) = 0;
  virtual void exitArgList(SparqlParser::ArgListContext *ctx) = 0;

  virtual void enterConstructTemplate(SparqlParser::ConstructTemplateContext *ctx) = 0;
  virtual void exitConstructTemplate(SparqlParser::ConstructTemplateContext *ctx) = 0;

  virtual void enterConstructTriples(SparqlParser::ConstructTriplesContext *ctx) = 0;
  virtual void exitConstructTriples(SparqlParser::ConstructTriplesContext *ctx) = 0;

  virtual void enterTriplesSameSubject(SparqlParser::TriplesSameSubjectContext *ctx) = 0;
  virtual void exitTriplesSameSubject(SparqlParser::TriplesSameSubjectContext *ctx) = 0;

  virtual void enterPropertyListNotEmpty(SparqlParser::PropertyListNotEmptyContext *ctx) = 0;
  virtual void exitPropertyListNotEmpty(SparqlParser::PropertyListNotEmptyContext *ctx) = 0;

  virtual void enterPropertyList(SparqlParser::PropertyListContext *ctx) = 0;
  virtual void exitPropertyList(SparqlParser::PropertyListContext *ctx) = 0;

  virtual void enterObjectList(SparqlParser::ObjectListContext *ctx) = 0;
  virtual void exitObjectList(SparqlParser::ObjectListContext *ctx) = 0;

  virtual void enterObject_(SparqlParser::Object_Context *ctx) = 0;
  virtual void exitObject_(SparqlParser::Object_Context *ctx) = 0;

  virtual void enterVerb(SparqlParser::VerbContext *ctx) = 0;
  virtual void exitVerb(SparqlParser::VerbContext *ctx) = 0;

  virtual void enterTriplesNode(SparqlParser::TriplesNodeContext *ctx) = 0;
  virtual void exitTriplesNode(SparqlParser::TriplesNodeContext *ctx) = 0;

  virtual void enterBlankNodePropertyList(SparqlParser::BlankNodePropertyListContext *ctx) = 0;
  virtual void exitBlankNodePropertyList(SparqlParser::BlankNodePropertyListContext *ctx) = 0;

  virtual void enterCollection(SparqlParser::CollectionContext *ctx) = 0;
  virtual void exitCollection(SparqlParser::CollectionContext *ctx) = 0;

  virtual void enterGraphNode(SparqlParser::GraphNodeContext *ctx) = 0;
  virtual void exitGraphNode(SparqlParser::GraphNodeContext *ctx) = 0;

  virtual void enterVarOrTerm(SparqlParser::VarOrTermContext *ctx) = 0;
  virtual void exitVarOrTerm(SparqlParser::VarOrTermContext *ctx) = 0;

  virtual void enterVarOrIRIref(SparqlParser::VarOrIRIrefContext *ctx) = 0;
  virtual void exitVarOrIRIref(SparqlParser::VarOrIRIrefContext *ctx) = 0;

  virtual void enterVar_(SparqlParser::Var_Context *ctx) = 0;
  virtual void exitVar_(SparqlParser::Var_Context *ctx) = 0;

  virtual void enterGraphTerm(SparqlParser::GraphTermContext *ctx) = 0;
  virtual void exitGraphTerm(SparqlParser::GraphTermContext *ctx) = 0;

  virtual void enterExpression(SparqlParser::ExpressionContext *ctx) = 0;
  virtual void exitExpression(SparqlParser::ExpressionContext *ctx) = 0;

  virtual void enterConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext *ctx) = 0;
  virtual void exitConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext *ctx) = 0;

  virtual void enterConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext *ctx) = 0;
  virtual void exitConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext *ctx) = 0;

  virtual void enterValueLogical(SparqlParser::ValueLogicalContext *ctx) = 0;
  virtual void exitValueLogical(SparqlParser::ValueLogicalContext *ctx) = 0;

  virtual void enterRelationalExpression(SparqlParser::RelationalExpressionContext *ctx) = 0;
  virtual void exitRelationalExpression(SparqlParser::RelationalExpressionContext *ctx) = 0;

  virtual void enterNumericExpression(SparqlParser::NumericExpressionContext *ctx) = 0;
  virtual void exitNumericExpression(SparqlParser::NumericExpressionContext *ctx) = 0;

  virtual void enterAdditiveExpression(SparqlParser::AdditiveExpressionContext *ctx) = 0;
  virtual void exitAdditiveExpression(SparqlParser::AdditiveExpressionContext *ctx) = 0;

  virtual void enterMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext *ctx) = 0;
  virtual void exitMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext *ctx) = 0;

  virtual void enterUnaryExpression(SparqlParser::UnaryExpressionContext *ctx) = 0;
  virtual void exitUnaryExpression(SparqlParser::UnaryExpressionContext *ctx) = 0;

  virtual void enterPrimaryExpression(SparqlParser::PrimaryExpressionContext *ctx) = 0;
  virtual void exitPrimaryExpression(SparqlParser::PrimaryExpressionContext *ctx) = 0;

  virtual void enterBrackettedExpression(SparqlParser::BrackettedExpressionContext *ctx) = 0;
  virtual void exitBrackettedExpression(SparqlParser::BrackettedExpressionContext *ctx) = 0;

  virtual void enterBuiltInCall(SparqlParser::BuiltInCallContext *ctx) = 0;
  virtual void exitBuiltInCall(SparqlParser::BuiltInCallContext *ctx) = 0;

  virtual void enterRegexExpression(SparqlParser::RegexExpressionContext *ctx) = 0;
  virtual void exitRegexExpression(SparqlParser::RegexExpressionContext *ctx) = 0;

  virtual void enterIriRefOrFunction(SparqlParser::IriRefOrFunctionContext *ctx) = 0;
  virtual void exitIriRefOrFunction(SparqlParser::IriRefOrFunctionContext *ctx) = 0;

  virtual void enterRdfLiteral(SparqlParser::RdfLiteralContext *ctx) = 0;
  virtual void exitRdfLiteral(SparqlParser::RdfLiteralContext *ctx) = 0;

  virtual void enterNumericLiteral(SparqlParser::NumericLiteralContext *ctx) = 0;
  virtual void exitNumericLiteral(SparqlParser::NumericLiteralContext *ctx) = 0;

  virtual void enterNumericLiteralUnsigned(SparqlParser::NumericLiteralUnsignedContext *ctx) = 0;
  virtual void exitNumericLiteralUnsigned(SparqlParser::NumericLiteralUnsignedContext *ctx) = 0;

  virtual void enterNumericLiteralPositive(SparqlParser::NumericLiteralPositiveContext *ctx) = 0;
  virtual void exitNumericLiteralPositive(SparqlParser::NumericLiteralPositiveContext *ctx) = 0;

  virtual void enterNumericLiteralNegative(SparqlParser::NumericLiteralNegativeContext *ctx) = 0;
  virtual void exitNumericLiteralNegative(SparqlParser::NumericLiteralNegativeContext *ctx) = 0;

  virtual void enterBooleanLiteral(SparqlParser::BooleanLiteralContext *ctx) = 0;
  virtual void exitBooleanLiteral(SparqlParser::BooleanLiteralContext *ctx) = 0;

  virtual void enterString_(SparqlParser::String_Context *ctx) = 0;
  virtual void exitString_(SparqlParser::String_Context *ctx) = 0;

  virtual void enterIriRef(SparqlParser::IriRefContext *ctx) = 0;
  virtual void exitIriRef(SparqlParser::IriRefContext *ctx) = 0;

  virtual void enterPrefixedName(SparqlParser::PrefixedNameContext *ctx) = 0;
  virtual void exitPrefixedName(SparqlParser::PrefixedNameContext *ctx) = 0;

  virtual void enterBlankNode(SparqlParser::BlankNodeContext *ctx) = 0;
  virtual void exitBlankNode(SparqlParser::BlankNodeContext *ctx) = 0;


};

}  // namespace antlr4_grammars
