
// Generated from SparqlParser.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "SparqlParserListener.h"


namespace antlr4_grammars {

/**
 * This class provides an empty implementation of SparqlParserListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  SparqlParserBaseListener : public SparqlParserListener {
public:

  virtual void enterQuery(SparqlParser::QueryContext * /*ctx*/) override { }
  virtual void exitQuery(SparqlParser::QueryContext * /*ctx*/) override { }

  virtual void enterPrologue(SparqlParser::PrologueContext * /*ctx*/) override { }
  virtual void exitPrologue(SparqlParser::PrologueContext * /*ctx*/) override { }

  virtual void enterBaseDecl(SparqlParser::BaseDeclContext * /*ctx*/) override { }
  virtual void exitBaseDecl(SparqlParser::BaseDeclContext * /*ctx*/) override { }

  virtual void enterPrefixDecl(SparqlParser::PrefixDeclContext * /*ctx*/) override { }
  virtual void exitPrefixDecl(SparqlParser::PrefixDeclContext * /*ctx*/) override { }

  virtual void enterSelectQuery(SparqlParser::SelectQueryContext * /*ctx*/) override { }
  virtual void exitSelectQuery(SparqlParser::SelectQueryContext * /*ctx*/) override { }

  virtual void enterConstructQuery(SparqlParser::ConstructQueryContext * /*ctx*/) override { }
  virtual void exitConstructQuery(SparqlParser::ConstructQueryContext * /*ctx*/) override { }

  virtual void enterDescribeQuery(SparqlParser::DescribeQueryContext * /*ctx*/) override { }
  virtual void exitDescribeQuery(SparqlParser::DescribeQueryContext * /*ctx*/) override { }

  virtual void enterAskQuery(SparqlParser::AskQueryContext * /*ctx*/) override { }
  virtual void exitAskQuery(SparqlParser::AskQueryContext * /*ctx*/) override { }

  virtual void enterDatasetClause(SparqlParser::DatasetClauseContext * /*ctx*/) override { }
  virtual void exitDatasetClause(SparqlParser::DatasetClauseContext * /*ctx*/) override { }

  virtual void enterDefaultGraphClause(SparqlParser::DefaultGraphClauseContext * /*ctx*/) override { }
  virtual void exitDefaultGraphClause(SparqlParser::DefaultGraphClauseContext * /*ctx*/) override { }

  virtual void enterNamedGraphClause(SparqlParser::NamedGraphClauseContext * /*ctx*/) override { }
  virtual void exitNamedGraphClause(SparqlParser::NamedGraphClauseContext * /*ctx*/) override { }

  virtual void enterSourceSelector(SparqlParser::SourceSelectorContext * /*ctx*/) override { }
  virtual void exitSourceSelector(SparqlParser::SourceSelectorContext * /*ctx*/) override { }

  virtual void enterWhereClause(SparqlParser::WhereClauseContext * /*ctx*/) override { }
  virtual void exitWhereClause(SparqlParser::WhereClauseContext * /*ctx*/) override { }

  virtual void enterSolutionModifier(SparqlParser::SolutionModifierContext * /*ctx*/) override { }
  virtual void exitSolutionModifier(SparqlParser::SolutionModifierContext * /*ctx*/) override { }

  virtual void enterLimitOffsetClauses(SparqlParser::LimitOffsetClausesContext * /*ctx*/) override { }
  virtual void exitLimitOffsetClauses(SparqlParser::LimitOffsetClausesContext * /*ctx*/) override { }

  virtual void enterOrderClause(SparqlParser::OrderClauseContext * /*ctx*/) override { }
  virtual void exitOrderClause(SparqlParser::OrderClauseContext * /*ctx*/) override { }

  virtual void enterOrderCondition(SparqlParser::OrderConditionContext * /*ctx*/) override { }
  virtual void exitOrderCondition(SparqlParser::OrderConditionContext * /*ctx*/) override { }

  virtual void enterLimitClause(SparqlParser::LimitClauseContext * /*ctx*/) override { }
  virtual void exitLimitClause(SparqlParser::LimitClauseContext * /*ctx*/) override { }

  virtual void enterOffsetClause(SparqlParser::OffsetClauseContext * /*ctx*/) override { }
  virtual void exitOffsetClause(SparqlParser::OffsetClauseContext * /*ctx*/) override { }

  virtual void enterGroupGraphPattern(SparqlParser::GroupGraphPatternContext * /*ctx*/) override { }
  virtual void exitGroupGraphPattern(SparqlParser::GroupGraphPatternContext * /*ctx*/) override { }

  virtual void enterTriplesBlock(SparqlParser::TriplesBlockContext * /*ctx*/) override { }
  virtual void exitTriplesBlock(SparqlParser::TriplesBlockContext * /*ctx*/) override { }

  virtual void enterGraphPatternNotTriples(SparqlParser::GraphPatternNotTriplesContext * /*ctx*/) override { }
  virtual void exitGraphPatternNotTriples(SparqlParser::GraphPatternNotTriplesContext * /*ctx*/) override { }

  virtual void enterOptionalGraphPattern(SparqlParser::OptionalGraphPatternContext * /*ctx*/) override { }
  virtual void exitOptionalGraphPattern(SparqlParser::OptionalGraphPatternContext * /*ctx*/) override { }

  virtual void enterGraphGraphPattern(SparqlParser::GraphGraphPatternContext * /*ctx*/) override { }
  virtual void exitGraphGraphPattern(SparqlParser::GraphGraphPatternContext * /*ctx*/) override { }

  virtual void enterGroupOrUnionGraphPattern(SparqlParser::GroupOrUnionGraphPatternContext * /*ctx*/) override { }
  virtual void exitGroupOrUnionGraphPattern(SparqlParser::GroupOrUnionGraphPatternContext * /*ctx*/) override { }

  virtual void enterFilter_(SparqlParser::Filter_Context * /*ctx*/) override { }
  virtual void exitFilter_(SparqlParser::Filter_Context * /*ctx*/) override { }

  virtual void enterConstraint(SparqlParser::ConstraintContext * /*ctx*/) override { }
  virtual void exitConstraint(SparqlParser::ConstraintContext * /*ctx*/) override { }

  virtual void enterFunctionCall(SparqlParser::FunctionCallContext * /*ctx*/) override { }
  virtual void exitFunctionCall(SparqlParser::FunctionCallContext * /*ctx*/) override { }

  virtual void enterArgList(SparqlParser::ArgListContext * /*ctx*/) override { }
  virtual void exitArgList(SparqlParser::ArgListContext * /*ctx*/) override { }

  virtual void enterConstructTemplate(SparqlParser::ConstructTemplateContext * /*ctx*/) override { }
  virtual void exitConstructTemplate(SparqlParser::ConstructTemplateContext * /*ctx*/) override { }

  virtual void enterConstructTriples(SparqlParser::ConstructTriplesContext * /*ctx*/) override { }
  virtual void exitConstructTriples(SparqlParser::ConstructTriplesContext * /*ctx*/) override { }

  virtual void enterTriplesSameSubject(SparqlParser::TriplesSameSubjectContext * /*ctx*/) override { }
  virtual void exitTriplesSameSubject(SparqlParser::TriplesSameSubjectContext * /*ctx*/) override { }

  virtual void enterPropertyListNotEmpty(SparqlParser::PropertyListNotEmptyContext * /*ctx*/) override { }
  virtual void exitPropertyListNotEmpty(SparqlParser::PropertyListNotEmptyContext * /*ctx*/) override { }

  virtual void enterPropertyList(SparqlParser::PropertyListContext * /*ctx*/) override { }
  virtual void exitPropertyList(SparqlParser::PropertyListContext * /*ctx*/) override { }

  virtual void enterObjectList(SparqlParser::ObjectListContext * /*ctx*/) override { }
  virtual void exitObjectList(SparqlParser::ObjectListContext * /*ctx*/) override { }

  virtual void enterObject_(SparqlParser::Object_Context * /*ctx*/) override { }
  virtual void exitObject_(SparqlParser::Object_Context * /*ctx*/) override { }

  virtual void enterVerb(SparqlParser::VerbContext * /*ctx*/) override { }
  virtual void exitVerb(SparqlParser::VerbContext * /*ctx*/) override { }

  virtual void enterTriplesNode(SparqlParser::TriplesNodeContext * /*ctx*/) override { }
  virtual void exitTriplesNode(SparqlParser::TriplesNodeContext * /*ctx*/) override { }

  virtual void enterBlankNodePropertyList(SparqlParser::BlankNodePropertyListContext * /*ctx*/) override { }
  virtual void exitBlankNodePropertyList(SparqlParser::BlankNodePropertyListContext * /*ctx*/) override { }

  virtual void enterCollection(SparqlParser::CollectionContext * /*ctx*/) override { }
  virtual void exitCollection(SparqlParser::CollectionContext * /*ctx*/) override { }

  virtual void enterGraphNode(SparqlParser::GraphNodeContext * /*ctx*/) override { }
  virtual void exitGraphNode(SparqlParser::GraphNodeContext * /*ctx*/) override { }

  virtual void enterVarOrTerm(SparqlParser::VarOrTermContext * /*ctx*/) override { }
  virtual void exitVarOrTerm(SparqlParser::VarOrTermContext * /*ctx*/) override { }

  virtual void enterVarOrIRIref(SparqlParser::VarOrIRIrefContext * /*ctx*/) override { }
  virtual void exitVarOrIRIref(SparqlParser::VarOrIRIrefContext * /*ctx*/) override { }

  virtual void enterVar_(SparqlParser::Var_Context * /*ctx*/) override { }
  virtual void exitVar_(SparqlParser::Var_Context * /*ctx*/) override { }

  virtual void enterGraphTerm(SparqlParser::GraphTermContext * /*ctx*/) override { }
  virtual void exitGraphTerm(SparqlParser::GraphTermContext * /*ctx*/) override { }

  virtual void enterExpression(SparqlParser::ExpressionContext * /*ctx*/) override { }
  virtual void exitExpression(SparqlParser::ExpressionContext * /*ctx*/) override { }

  virtual void enterConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext * /*ctx*/) override { }
  virtual void exitConditionalOrExpression(SparqlParser::ConditionalOrExpressionContext * /*ctx*/) override { }

  virtual void enterConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext * /*ctx*/) override { }
  virtual void exitConditionalAndExpression(SparqlParser::ConditionalAndExpressionContext * /*ctx*/) override { }

  virtual void enterValueLogical(SparqlParser::ValueLogicalContext * /*ctx*/) override { }
  virtual void exitValueLogical(SparqlParser::ValueLogicalContext * /*ctx*/) override { }

  virtual void enterRelationalExpression(SparqlParser::RelationalExpressionContext * /*ctx*/) override { }
  virtual void exitRelationalExpression(SparqlParser::RelationalExpressionContext * /*ctx*/) override { }

  virtual void enterNumericExpression(SparqlParser::NumericExpressionContext * /*ctx*/) override { }
  virtual void exitNumericExpression(SparqlParser::NumericExpressionContext * /*ctx*/) override { }

  virtual void enterAdditiveExpression(SparqlParser::AdditiveExpressionContext * /*ctx*/) override { }
  virtual void exitAdditiveExpression(SparqlParser::AdditiveExpressionContext * /*ctx*/) override { }

  virtual void enterMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext * /*ctx*/) override { }
  virtual void exitMultiplicativeExpression(SparqlParser::MultiplicativeExpressionContext * /*ctx*/) override { }

  virtual void enterUnaryExpression(SparqlParser::UnaryExpressionContext * /*ctx*/) override { }
  virtual void exitUnaryExpression(SparqlParser::UnaryExpressionContext * /*ctx*/) override { }

  virtual void enterPrimaryExpression(SparqlParser::PrimaryExpressionContext * /*ctx*/) override { }
  virtual void exitPrimaryExpression(SparqlParser::PrimaryExpressionContext * /*ctx*/) override { }

  virtual void enterBrackettedExpression(SparqlParser::BrackettedExpressionContext * /*ctx*/) override { }
  virtual void exitBrackettedExpression(SparqlParser::BrackettedExpressionContext * /*ctx*/) override { }

  virtual void enterBuiltInCall(SparqlParser::BuiltInCallContext * /*ctx*/) override { }
  virtual void exitBuiltInCall(SparqlParser::BuiltInCallContext * /*ctx*/) override { }

  virtual void enterRegexExpression(SparqlParser::RegexExpressionContext * /*ctx*/) override { }
  virtual void exitRegexExpression(SparqlParser::RegexExpressionContext * /*ctx*/) override { }

  virtual void enterIriRefOrFunction(SparqlParser::IriRefOrFunctionContext * /*ctx*/) override { }
  virtual void exitIriRefOrFunction(SparqlParser::IriRefOrFunctionContext * /*ctx*/) override { }

  virtual void enterRdfLiteral(SparqlParser::RdfLiteralContext * /*ctx*/) override { }
  virtual void exitRdfLiteral(SparqlParser::RdfLiteralContext * /*ctx*/) override { }

  virtual void enterNumericLiteral(SparqlParser::NumericLiteralContext * /*ctx*/) override { }
  virtual void exitNumericLiteral(SparqlParser::NumericLiteralContext * /*ctx*/) override { }

  virtual void enterNumericLiteralUnsigned(SparqlParser::NumericLiteralUnsignedContext * /*ctx*/) override { }
  virtual void exitNumericLiteralUnsigned(SparqlParser::NumericLiteralUnsignedContext * /*ctx*/) override { }

  virtual void enterNumericLiteralPositive(SparqlParser::NumericLiteralPositiveContext * /*ctx*/) override { }
  virtual void exitNumericLiteralPositive(SparqlParser::NumericLiteralPositiveContext * /*ctx*/) override { }

  virtual void enterNumericLiteralNegative(SparqlParser::NumericLiteralNegativeContext * /*ctx*/) override { }
  virtual void exitNumericLiteralNegative(SparqlParser::NumericLiteralNegativeContext * /*ctx*/) override { }

  virtual void enterBooleanLiteral(SparqlParser::BooleanLiteralContext * /*ctx*/) override { }
  virtual void exitBooleanLiteral(SparqlParser::BooleanLiteralContext * /*ctx*/) override { }

  virtual void enterString_(SparqlParser::String_Context * /*ctx*/) override { }
  virtual void exitString_(SparqlParser::String_Context * /*ctx*/) override { }

  virtual void enterIriRef(SparqlParser::IriRefContext * /*ctx*/) override { }
  virtual void exitIriRef(SparqlParser::IriRefContext * /*ctx*/) override { }

  virtual void enterPrefixedName(SparqlParser::PrefixedNameContext * /*ctx*/) override { }
  virtual void exitPrefixedName(SparqlParser::PrefixedNameContext * /*ctx*/) override { }

  virtual void enterBlankNode(SparqlParser::BlankNodeContext * /*ctx*/) override { }
  virtual void exitBlankNode(SparqlParser::BlankNodeContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

}  // namespace antlr4_grammars
