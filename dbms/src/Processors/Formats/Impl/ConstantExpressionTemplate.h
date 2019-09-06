#pragma once

#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Formats/FormatSettings.h>
#include <Parsers/TokenIterator.h>

namespace DB
{

struct LiteralInfo;

/// Deduces template of an expression by replacing literals with dummy columns.
/// It allows to parse and evaluate similar expressions without using heavy IParsers and ExpressionAnalyzer.
/// Using ConstantExpressionTemplate for one expression is slower then evaluateConstantExpression(...),
/// but it's significantly faster for batch of expressions
class ConstantExpressionTemplate
{
public:
    /// Deduce template of expression of type result_column_type
    ConstantExpressionTemplate(DataTypePtr  result_column_type_, TokenIterator expression_begin, TokenIterator expression_end,
                               const ASTPtr & expression, const Context & context);

    /// Read expression from istr, assert it has the same structure and the same types of literals (template matches)
    /// and parse literals into temporary columns
    void parseExpression(ReadBuffer & istr, const FormatSettings & settings);

    /// Evaluate batch of expressions were parsed using template
    ColumnPtr evaluateAll();

    size_t rowsCount() const { return rows_count; }

private:
    static void addNodesToCastResult(const IDataType & result_column_type, ASTPtr & expr);
    bool getDataType(const LiteralInfo & info, DataTypePtr & type) const;
    void parseLiteralAndAssertType(ReadBuffer & istr, const IDataType * type, size_t column_idx);

private:
    DataTypePtr result_column_type;

    std::vector<String> tokens;
    std::vector<size_t> token_after_literal_idx;

    String result_column_name;
    ExpressionActionsPtr actions_on_literals;
    Block literals;
    MutableColumns columns;

    std::vector<char> use_special_parser;

    /// For expressions without literals (e.g. "now()")
    size_t rows_count = 0;

};

}
