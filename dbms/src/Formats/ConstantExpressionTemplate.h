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
    ConstantExpressionTemplate(const IDataType & result_column_type, TokenIterator begin, TokenIterator end, const Context & context);

    void parseExpression(ReadBuffer & istr, const FormatSettings & settings);

    ColumnPtr evaluateAll();

private:
    std::pair<String, NamesAndTypesList> replaceLiteralsWithDummyIdentifiers(TokenIterator & begin, TokenIterator & end, const IDataType & result_column_type);

    static void addNodesToCastResult(const IDataType & result_column_type, ASTPtr & expr);

private:
    std::vector<String> tokens;
    std::vector<size_t> token_after_literal_idx;
    String result_column_name;
    ExpressionActionsPtr actions_on_literals;
    Block literals;
    MutableColumns columns;

    /// For expressions without literals (e.g. "now()")
    size_t rows_count = 0;

};

}
