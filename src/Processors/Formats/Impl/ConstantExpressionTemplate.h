#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct LiteralInfo;
using LiteralsInfo = std::vector<LiteralInfo>;
struct SpecialParserType;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Deduces template of an expression by replacing literals with dummy columns.
/// It allows to parse and evaluate similar expressions without using heavy IParsers and ExpressionAnalyzer.
/// Using ConstantExpressionTemplate for one expression is slower then evaluateConstantExpression(...),
/// but it's significantly faster for batch of expressions
class ConstantExpressionTemplate : boost::noncopyable
{
    struct TemplateStructure : boost::noncopyable
    {
        TemplateStructure(LiteralsInfo & replaced_literals, TokenIterator expression_begin, TokenIterator expression_end,
                          ASTPtr & expr, const IDataType & result_type, bool null_as_default_, ContextPtr context);

        static void addNodesToCastResult(const IDataType & result_column_type, ASTPtr & expr, bool null_as_default);
        static size_t getTemplateHash(const ASTPtr & expression, const LiteralsInfo & replaced_literals,
                                      const DataTypePtr & result_column_type, bool null_as_default, const String & salt);

        String dumpTemplate() const;

        String result_column_name;

        std::vector<String> tokens;
        std::vector<size_t> token_after_literal_idx;

        Block literals;
        ExpressionActionsPtr actions_on_literals;
        Serializations serializations;

        std::vector<SpecialParserType> special_parser;
        bool null_as_default;
    };

public:
    using TemplateStructurePtr = std::shared_ptr<const TemplateStructure>;

    class Cache : boost::noncopyable
    {
        std::unordered_map<size_t, TemplateStructurePtr> cache;
        const size_t max_size;

    public:
        explicit Cache(size_t max_size_ = 4096) : max_size(max_size_) {}

        /// Deduce template of expression of type result_column_type and add it to cache (or use template from cache)
        TemplateStructurePtr getFromCacheOrConstruct(const DataTypePtr & result_column_type,
                                                     bool null_as_default,
                                                     TokenIterator expression_begin,
                                                     TokenIterator expression_end,
                                                     const ASTPtr & expression_,
                                                     ContextPtr context,
                                                     bool * found_in_cache = nullptr,
                                                     const String & salt = {});
    };

    explicit ConstantExpressionTemplate(const TemplateStructurePtr & structure_)
            : structure(structure_), columns(structure->literals.cloneEmptyColumns()) {}

    /// Read expression from istr, assert it has the same structure and the same types of literals (template matches)
    /// and parse literals into temporary columns
    bool parseExpression(
        ReadBuffer & istr, const TokenIterator & token_iterator, const FormatSettings & format_settings, const Settings & settings);

    /// Evaluate batch of expressions were parsed using template.
    /// If template was deduced with null_as_default == true, set bits in nulls for NULL values in column_idx, starting from offset.
    ColumnPtr evaluateAll(BlockMissingValues & nulls, size_t column_idx, const DataTypePtr & expected_type, size_t offset = 0);

    size_t rowsCount() const { return rows_count; }

private:
    bool tryParseExpression(
        ReadBuffer & istr,
        const TokenIterator & token_iterator,
        const FormatSettings & format_settings,
        size_t & cur_column,
        const Settings & settings);
    bool parseLiteralAndAssertType(
        ReadBuffer & istr, const TokenIterator & token_iterator, const IDataType * type, size_t column_idx, const Settings & settings);

    TemplateStructurePtr structure;
    MutableColumns columns;

    /// For expressions without literals (e.g. "now()")
    size_t rows_count = 0;

};

}
