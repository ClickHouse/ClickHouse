
// Generated from PromQLParser.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "PromQLParserVisitor.h"


namespace antlr4_grammars {

/**
 * This class provides an empty implementation of PromQLParserVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the available methods.
 */
class  PromQLParserBaseVisitor : public PromQLParserVisitor {
public:

  virtual std::any visitExpression(PromQLParser::ExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVectorOperation(PromQLParser::VectorOperationContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnaryOp(PromQLParser::UnaryOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPowOp(PromQLParser::PowOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultOp(PromQLParser::MultOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAddOp(PromQLParser::AddOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCompareOp(PromQLParser::CompareOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAndUnlessOp(PromQLParser::AndUnlessOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOrOp(PromQLParser::OrOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubqueryOp(PromQLParser::SubqueryOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOffsetOp(PromQLParser::OffsetOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVector(PromQLParser::VectorContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParens(PromQLParser::ParensContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInstantSelector(PromQLParser::InstantSelectorContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLabelMatcher(PromQLParser::LabelMatcherContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLabelMatcherOperator(PromQLParser::LabelMatcherOperatorContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLabelMatcherList(PromQLParser::LabelMatcherListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMatrixSelector(PromQLParser::MatrixSelectorContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOffset(PromQLParser::OffsetContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunction_(PromQLParser::Function_Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParameter(PromQLParser::ParameterContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParameterList(PromQLParser::ParameterListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAggregation(PromQLParser::AggregationContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBy(PromQLParser::ByContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWithout(PromQLParser::WithoutContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGrouping(PromQLParser::GroupingContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOn_(PromQLParser::On_Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIgnoring(PromQLParser::IgnoringContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupLeft(PromQLParser::GroupLeftContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupRight(PromQLParser::GroupRightContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLabelName(PromQLParser::LabelNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLabelNameList(PromQLParser::LabelNameListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMetricName(PromQLParser::MetricNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitKeyword(PromQLParser::KeywordContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLiteral(PromQLParser::LiteralContext *ctx) override {
    return visitChildren(ctx);
  }


};

}  // namespace antlr4_grammars
