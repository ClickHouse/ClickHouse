
// Generated from PromQLParser.g4 by ANTLR 4.13.2

#pragma once


#include "antlr4-runtime.h"
#include "PromQLParser.h"


namespace antlr4_grammars {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by PromQLParser.
 */
class  PromQLParserVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by PromQLParser.
   */
    virtual std::any visitExpression(PromQLParser::ExpressionContext *context) = 0;

    virtual std::any visitVectorOperation(PromQLParser::VectorOperationContext *context) = 0;

    virtual std::any visitUnaryOp(PromQLParser::UnaryOpContext *context) = 0;

    virtual std::any visitPowOp(PromQLParser::PowOpContext *context) = 0;

    virtual std::any visitMultOp(PromQLParser::MultOpContext *context) = 0;

    virtual std::any visitAddOp(PromQLParser::AddOpContext *context) = 0;

    virtual std::any visitCompareOp(PromQLParser::CompareOpContext *context) = 0;

    virtual std::any visitAndUnlessOp(PromQLParser::AndUnlessOpContext *context) = 0;

    virtual std::any visitOrOp(PromQLParser::OrOpContext *context) = 0;

    virtual std::any visitSubqueryOp(PromQLParser::SubqueryOpContext *context) = 0;

    virtual std::any visitOffsetOp(PromQLParser::OffsetOpContext *context) = 0;

    virtual std::any visitVector(PromQLParser::VectorContext *context) = 0;

    virtual std::any visitParens(PromQLParser::ParensContext *context) = 0;

    virtual std::any visitInstantSelector(PromQLParser::InstantSelectorContext *context) = 0;

    virtual std::any visitLabelMatcher(PromQLParser::LabelMatcherContext *context) = 0;

    virtual std::any visitLabelMatcherOperator(PromQLParser::LabelMatcherOperatorContext *context) = 0;

    virtual std::any visitLabelMatcherList(PromQLParser::LabelMatcherListContext *context) = 0;

    virtual std::any visitMatrixSelector(PromQLParser::MatrixSelectorContext *context) = 0;

    virtual std::any visitOffset(PromQLParser::OffsetContext *context) = 0;

    virtual std::any visitFunction_(PromQLParser::Function_Context *context) = 0;

    virtual std::any visitParameter(PromQLParser::ParameterContext *context) = 0;

    virtual std::any visitParameterList(PromQLParser::ParameterListContext *context) = 0;

    virtual std::any visitAggregation(PromQLParser::AggregationContext *context) = 0;

    virtual std::any visitBy(PromQLParser::ByContext *context) = 0;

    virtual std::any visitWithout(PromQLParser::WithoutContext *context) = 0;

    virtual std::any visitGrouping(PromQLParser::GroupingContext *context) = 0;

    virtual std::any visitOn_(PromQLParser::On_Context *context) = 0;

    virtual std::any visitIgnoring(PromQLParser::IgnoringContext *context) = 0;

    virtual std::any visitGroupLeft(PromQLParser::GroupLeftContext *context) = 0;

    virtual std::any visitGroupRight(PromQLParser::GroupRightContext *context) = 0;

    virtual std::any visitLabelName(PromQLParser::LabelNameContext *context) = 0;

    virtual std::any visitLabelNameList(PromQLParser::LabelNameListContext *context) = 0;

    virtual std::any visitMetricName(PromQLParser::MetricNameContext *context) = 0;

    virtual std::any visitKeyword(PromQLParser::KeywordContext *context) = 0;

    virtual std::any visitLiteral(PromQLParser::LiteralContext *context) = 0;


};

}  // namespace antlr4_grammars
