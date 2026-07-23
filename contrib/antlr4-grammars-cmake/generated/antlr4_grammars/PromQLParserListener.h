
// Generated from PromQLParser.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "PromQLParser.h"


namespace antlr4_grammars {

/**
 * This interface defines an abstract listener for a parse tree produced by PromQLParser.
 */
class  PromQLParserListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterExpression(PromQLParser::ExpressionContext *ctx) = 0;
  virtual void exitExpression(PromQLParser::ExpressionContext *ctx) = 0;

  virtual void enterVectorOperation(PromQLParser::VectorOperationContext *ctx) = 0;
  virtual void exitVectorOperation(PromQLParser::VectorOperationContext *ctx) = 0;

  virtual void enterUnaryOp(PromQLParser::UnaryOpContext *ctx) = 0;
  virtual void exitUnaryOp(PromQLParser::UnaryOpContext *ctx) = 0;

  virtual void enterPowOp(PromQLParser::PowOpContext *ctx) = 0;
  virtual void exitPowOp(PromQLParser::PowOpContext *ctx) = 0;

  virtual void enterMultOp(PromQLParser::MultOpContext *ctx) = 0;
  virtual void exitMultOp(PromQLParser::MultOpContext *ctx) = 0;

  virtual void enterAddOp(PromQLParser::AddOpContext *ctx) = 0;
  virtual void exitAddOp(PromQLParser::AddOpContext *ctx) = 0;

  virtual void enterCompareOp(PromQLParser::CompareOpContext *ctx) = 0;
  virtual void exitCompareOp(PromQLParser::CompareOpContext *ctx) = 0;

  virtual void enterAndUnlessOp(PromQLParser::AndUnlessOpContext *ctx) = 0;
  virtual void exitAndUnlessOp(PromQLParser::AndUnlessOpContext *ctx) = 0;

  virtual void enterOrOp(PromQLParser::OrOpContext *ctx) = 0;
  virtual void exitOrOp(PromQLParser::OrOpContext *ctx) = 0;

  virtual void enterSubqueryOp(PromQLParser::SubqueryOpContext *ctx) = 0;
  virtual void exitSubqueryOp(PromQLParser::SubqueryOpContext *ctx) = 0;

  virtual void enterOffsetOp(PromQLParser::OffsetOpContext *ctx) = 0;
  virtual void exitOffsetOp(PromQLParser::OffsetOpContext *ctx) = 0;

  virtual void enterVector(PromQLParser::VectorContext *ctx) = 0;
  virtual void exitVector(PromQLParser::VectorContext *ctx) = 0;

  virtual void enterParens(PromQLParser::ParensContext *ctx) = 0;
  virtual void exitParens(PromQLParser::ParensContext *ctx) = 0;

  virtual void enterInstantSelector(PromQLParser::InstantSelectorContext *ctx) = 0;
  virtual void exitInstantSelector(PromQLParser::InstantSelectorContext *ctx) = 0;

  virtual void enterLabelMatcher(PromQLParser::LabelMatcherContext *ctx) = 0;
  virtual void exitLabelMatcher(PromQLParser::LabelMatcherContext *ctx) = 0;

  virtual void enterLabelMatcherOperator(PromQLParser::LabelMatcherOperatorContext *ctx) = 0;
  virtual void exitLabelMatcherOperator(PromQLParser::LabelMatcherOperatorContext *ctx) = 0;

  virtual void enterLabelMatcherList(PromQLParser::LabelMatcherListContext *ctx) = 0;
  virtual void exitLabelMatcherList(PromQLParser::LabelMatcherListContext *ctx) = 0;

  virtual void enterMatrixSelector(PromQLParser::MatrixSelectorContext *ctx) = 0;
  virtual void exitMatrixSelector(PromQLParser::MatrixSelectorContext *ctx) = 0;

  virtual void enterOffset(PromQLParser::OffsetContext *ctx) = 0;
  virtual void exitOffset(PromQLParser::OffsetContext *ctx) = 0;

  virtual void enterFunction_(PromQLParser::Function_Context *ctx) = 0;
  virtual void exitFunction_(PromQLParser::Function_Context *ctx) = 0;

  virtual void enterParameter(PromQLParser::ParameterContext *ctx) = 0;
  virtual void exitParameter(PromQLParser::ParameterContext *ctx) = 0;

  virtual void enterParameterList(PromQLParser::ParameterListContext *ctx) = 0;
  virtual void exitParameterList(PromQLParser::ParameterListContext *ctx) = 0;

  virtual void enterAggregation(PromQLParser::AggregationContext *ctx) = 0;
  virtual void exitAggregation(PromQLParser::AggregationContext *ctx) = 0;

  virtual void enterBy(PromQLParser::ByContext *ctx) = 0;
  virtual void exitBy(PromQLParser::ByContext *ctx) = 0;

  virtual void enterWithout(PromQLParser::WithoutContext *ctx) = 0;
  virtual void exitWithout(PromQLParser::WithoutContext *ctx) = 0;

  virtual void enterGrouping(PromQLParser::GroupingContext *ctx) = 0;
  virtual void exitGrouping(PromQLParser::GroupingContext *ctx) = 0;

  virtual void enterOn_(PromQLParser::On_Context *ctx) = 0;
  virtual void exitOn_(PromQLParser::On_Context *ctx) = 0;

  virtual void enterIgnoring(PromQLParser::IgnoringContext *ctx) = 0;
  virtual void exitIgnoring(PromQLParser::IgnoringContext *ctx) = 0;

  virtual void enterGroupLeft(PromQLParser::GroupLeftContext *ctx) = 0;
  virtual void exitGroupLeft(PromQLParser::GroupLeftContext *ctx) = 0;

  virtual void enterGroupRight(PromQLParser::GroupRightContext *ctx) = 0;
  virtual void exitGroupRight(PromQLParser::GroupRightContext *ctx) = 0;

  virtual void enterLabelName(PromQLParser::LabelNameContext *ctx) = 0;
  virtual void exitLabelName(PromQLParser::LabelNameContext *ctx) = 0;

  virtual void enterLabelNameList(PromQLParser::LabelNameListContext *ctx) = 0;
  virtual void exitLabelNameList(PromQLParser::LabelNameListContext *ctx) = 0;

  virtual void enterMetricName(PromQLParser::MetricNameContext *ctx) = 0;
  virtual void exitMetricName(PromQLParser::MetricNameContext *ctx) = 0;

  virtual void enterKeyword(PromQLParser::KeywordContext *ctx) = 0;
  virtual void exitKeyword(PromQLParser::KeywordContext *ctx) = 0;

  virtual void enterLiteral(PromQLParser::LiteralContext *ctx) = 0;
  virtual void exitLiteral(PromQLParser::LiteralContext *ctx) = 0;


};

}  // namespace antlr4_grammars
