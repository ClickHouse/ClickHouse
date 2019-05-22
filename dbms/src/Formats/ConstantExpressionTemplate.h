#pragma once

#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Formats/FormatSettings.h>
#include <Parsers/TokenIterator.h>

namespace DB
{

class ConstantExpressionTemplate
{
public:
    ConstantExpressionTemplate(const IDataType & result_column_type, TokenIterator expression_begin, TokenIterator expression_end,
                               const ASTPtr & expression, const Context & context);

    void parseExpression(ReadBuffer & istr, const FormatSettings & settings);

    ColumnPtr evaluateAll();

private:
    static void addNodesToCastResult(const IDataType & result_column_type, ASTPtr & expr);

private:
    std::vector<String> tokens;
    std::vector<size_t> token_after_literal_idx;

    String result_column_name;
    ExpressionActionsPtr actions_on_literals;
    Block literals;
    MutableColumns columns;

    std::vector<char> need_special_parser;

    /// For expressions without literals (e.g. "now()")
    size_t rows_count = 0;

};

}
