#pragma once

#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Formats/FormatSettings.h>
#include <Parsers/TokenIterator.h>

namespace DB
{

struct LiteralInfo;
using LiteralsInfo = std::vector<LiteralInfo>;

/// Deduces template of an expression by replacing literals with dummy columns.
/// It allows to parse and evaluate similar expressions without using heavy IParsers and ExpressionAnalyzer.
/// Using ConstantExpressionTemplate for one expression is slower then evaluateConstantExpression(...),
/// but it's significantly faster for batch of expressions
class ConstantExpressionTemplate
{
    struct TemplateStructure
    {
        TemplateStructure(LiteralsInfo & replaced_literals, TokenIterator expression_begin, TokenIterator expression_end,
                          ASTPtr & expr, const IDataType & result_type, const Context & context);

        static void addNodesToCastResult(const IDataType & result_column_type, ASTPtr & expr);
        static size_t getTemplateHash(const ASTPtr & expression, const LiteralsInfo & replaced_literals, const DataTypePtr & result_column_type, TokenIterator expression_end);

        String result_column_name;

        std::vector<String> tokens;
        std::vector<size_t> token_after_literal_idx;

        Block literals;
        ExpressionActionsPtr actions_on_literals;

        std::vector<char> use_special_parser;
    };

public:
    using TemplateStructurePtr = std::shared_ptr<TemplateStructure>;
    using Cache = std::unordered_map<size_t, TemplateStructurePtr>;

    static constexpr size_t max_cache_size = 4096;

    /// Deduce template of expression of type result_column_type
    ConstantExpressionTemplate(DataTypePtr  result_column_type_, TokenIterator expression_begin_, TokenIterator expression_end_,
                               const ASTPtr & expression_, const Context & context_, Cache * cache = nullptr);

    /// Read expression from istr, assert it has the same structure and the same types of literals (template matches)
    /// and parse literals into temporary columns
    bool parseExpression(ReadBuffer & istr, const FormatSettings & settings);

    /// Evaluate batch of expressions were parsed using template
    ColumnPtr evaluateAll();

    size_t rowsCount() const { return rows_count; }

private:
    bool tryParseExpression(ReadBuffer & istr, const FormatSettings & settings, size_t & cur_column);
    bool parseLiteralAndAssertType(ReadBuffer & istr, const IDataType * type, size_t column_idx);

private:
    TemplateStructurePtr structure;
    MutableColumns columns;

    /// For expressions without literals (e.g. "now()")
    size_t rows_count = 0;

};

}
