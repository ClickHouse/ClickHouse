
// Generated from PromQLParser.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "PromQLParserListener.h"


namespace antlr4_grammars {

/**
 * This class provides an empty implementation of PromQLParserListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  PromQLParserBaseListener : public PromQLParserListener {
public:

  virtual void enterExpression(PromQLParser::ExpressionContext * /*ctx*/) override { }
  virtual void exitExpression(PromQLParser::ExpressionContext * /*ctx*/) override { }

  virtual void enterVectorOperation(PromQLParser::VectorOperationContext * /*ctx*/) override { }
  virtual void exitVectorOperation(PromQLParser::VectorOperationContext * /*ctx*/) override { }

  virtual void enterUnaryOp(PromQLParser::UnaryOpContext * /*ctx*/) override { }
  virtual void exitUnaryOp(PromQLParser::UnaryOpContext * /*ctx*/) override { }

  virtual void enterPowOp(PromQLParser::PowOpContext * /*ctx*/) override { }
  virtual void exitPowOp(PromQLParser::PowOpContext * /*ctx*/) override { }

  virtual void enterMultOp(PromQLParser::MultOpContext * /*ctx*/) override { }
  virtual void exitMultOp(PromQLParser::MultOpContext * /*ctx*/) override { }

  virtual void enterAddOp(PromQLParser::AddOpContext * /*ctx*/) override { }
  virtual void exitAddOp(PromQLParser::AddOpContext * /*ctx*/) override { }

  virtual void enterCompareOp(PromQLParser::CompareOpContext * /*ctx*/) override { }
  virtual void exitCompareOp(PromQLParser::CompareOpContext * /*ctx*/) override { }

  virtual void enterAndUnlessOp(PromQLParser::AndUnlessOpContext * /*ctx*/) override { }
  virtual void exitAndUnlessOp(PromQLParser::AndUnlessOpContext * /*ctx*/) override { }

  virtual void enterOrOp(PromQLParser::OrOpContext * /*ctx*/) override { }
  virtual void exitOrOp(PromQLParser::OrOpContext * /*ctx*/) override { }

  virtual void enterSubqueryOp(PromQLParser::SubqueryOpContext * /*ctx*/) override { }
  virtual void exitSubqueryOp(PromQLParser::SubqueryOpContext * /*ctx*/) override { }

  virtual void enterOffsetOp(PromQLParser::OffsetOpContext * /*ctx*/) override { }
  virtual void exitOffsetOp(PromQLParser::OffsetOpContext * /*ctx*/) override { }

  virtual void enterVector(PromQLParser::VectorContext * /*ctx*/) override { }
  virtual void exitVector(PromQLParser::VectorContext * /*ctx*/) override { }

  virtual void enterParens(PromQLParser::ParensContext * /*ctx*/) override { }
  virtual void exitParens(PromQLParser::ParensContext * /*ctx*/) override { }

  virtual void enterInstantSelector(PromQLParser::InstantSelectorContext * /*ctx*/) override { }
  virtual void exitInstantSelector(PromQLParser::InstantSelectorContext * /*ctx*/) override { }

  virtual void enterLabelMatcher(PromQLParser::LabelMatcherContext * /*ctx*/) override { }
  virtual void exitLabelMatcher(PromQLParser::LabelMatcherContext * /*ctx*/) override { }

  virtual void enterLabelMatcherOperator(PromQLParser::LabelMatcherOperatorContext * /*ctx*/) override { }
  virtual void exitLabelMatcherOperator(PromQLParser::LabelMatcherOperatorContext * /*ctx*/) override { }

  virtual void enterLabelMatcherList(PromQLParser::LabelMatcherListContext * /*ctx*/) override { }
  virtual void exitLabelMatcherList(PromQLParser::LabelMatcherListContext * /*ctx*/) override { }

  virtual void enterMatrixSelector(PromQLParser::MatrixSelectorContext * /*ctx*/) override { }
  virtual void exitMatrixSelector(PromQLParser::MatrixSelectorContext * /*ctx*/) override { }

  virtual void enterOffset(PromQLParser::OffsetContext * /*ctx*/) override { }
  virtual void exitOffset(PromQLParser::OffsetContext * /*ctx*/) override { }

  virtual void enterFunction_(PromQLParser::Function_Context * /*ctx*/) override { }
  virtual void exitFunction_(PromQLParser::Function_Context * /*ctx*/) override { }

  virtual void enterParameter(PromQLParser::ParameterContext * /*ctx*/) override { }
  virtual void exitParameter(PromQLParser::ParameterContext * /*ctx*/) override { }

  virtual void enterParameterList(PromQLParser::ParameterListContext * /*ctx*/) override { }
  virtual void exitParameterList(PromQLParser::ParameterListContext * /*ctx*/) override { }

  virtual void enterAggregation(PromQLParser::AggregationContext * /*ctx*/) override { }
  virtual void exitAggregation(PromQLParser::AggregationContext * /*ctx*/) override { }

  virtual void enterBy(PromQLParser::ByContext * /*ctx*/) override { }
  virtual void exitBy(PromQLParser::ByContext * /*ctx*/) override { }

  virtual void enterWithout(PromQLParser::WithoutContext * /*ctx*/) override { }
  virtual void exitWithout(PromQLParser::WithoutContext * /*ctx*/) override { }

  virtual void enterGrouping(PromQLParser::GroupingContext * /*ctx*/) override { }
  virtual void exitGrouping(PromQLParser::GroupingContext * /*ctx*/) override { }

  virtual void enterOn_(PromQLParser::On_Context * /*ctx*/) override { }
  virtual void exitOn_(PromQLParser::On_Context * /*ctx*/) override { }

  virtual void enterIgnoring(PromQLParser::IgnoringContext * /*ctx*/) override { }
  virtual void exitIgnoring(PromQLParser::IgnoringContext * /*ctx*/) override { }

  virtual void enterGroupLeft(PromQLParser::GroupLeftContext * /*ctx*/) override { }
  virtual void exitGroupLeft(PromQLParser::GroupLeftContext * /*ctx*/) override { }

  virtual void enterGroupRight(PromQLParser::GroupRightContext * /*ctx*/) override { }
  virtual void exitGroupRight(PromQLParser::GroupRightContext * /*ctx*/) override { }

  virtual void enterLabelName(PromQLParser::LabelNameContext * /*ctx*/) override { }
  virtual void exitLabelName(PromQLParser::LabelNameContext * /*ctx*/) override { }

  virtual void enterLabelNameList(PromQLParser::LabelNameListContext * /*ctx*/) override { }
  virtual void exitLabelNameList(PromQLParser::LabelNameListContext * /*ctx*/) override { }

  virtual void enterMetricName(PromQLParser::MetricNameContext * /*ctx*/) override { }
  virtual void exitMetricName(PromQLParser::MetricNameContext * /*ctx*/) override { }

  virtual void enterKeyword(PromQLParser::KeywordContext * /*ctx*/) override { }
  virtual void exitKeyword(PromQLParser::KeywordContext * /*ctx*/) override { }

  virtual void enterLiteral(PromQLParser::LiteralContext * /*ctx*/) override { }
  virtual void exitLiteral(PromQLParser::LiteralContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

}  // namespace antlr4_grammars
