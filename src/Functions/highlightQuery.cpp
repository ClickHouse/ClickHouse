#include <Functions/FunctionFactory.h>
#include <Functions/QueryTokenizationImpl.h>
#include <Parsers/IParser.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/TokenIterator.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

struct HighlightQueryImpl
{
    static constexpr auto name = "highlightQuery";

    static DataTypePtr makeEnumType()
    {
#define ENUM_TYPE Highlight
        return MAKE_ENUM8_TYPE(APPLY_FOR_HIGHLIGHTS);
#undef ENUM_TYPE
    }

    static void processRow(
        std::string_view query,
        PaddedPODArray<UInt64> & data_begin,
        PaddedPODArray<UInt64> & data_end,
        PaddedPODArray<Int8> & data_type,
        size_t & total,
        const QueryTokenizationSettings & settings)
    {
        const char * begin = query.data();
        const char * end = begin + query.size();

        Tokens tokens(begin, end, /* max_query_size = */ 0, /* skip_insignificant = */ true);
        IParser::Pos token_iterator(tokens, static_cast<uint32_t>(settings.max_parser_depth), static_cast<uint32_t>(settings.max_parser_backtracks));

        Expected expected;
        expected.enable_highlighting = true;

        ParserQuery parser(end, /* allow_settings_after_format_in_insert = */ false, /* implicit_select = */ settings.implicit_select);
        ASTPtr ast;

        try
        {
            while (!token_iterator->isEnd())
            {
                bool res = parser.parse(token_iterator, ast, expected);
                if (!res)
                    break;

                if (!token_iterator->isEnd() && token_iterator->type != TokenType::Semicolon)
                    break;

                while (token_iterator->type == TokenType::Semicolon)
                    ++token_iterator;
            }
        }
        catch (const Exception & e)
        {
            /// Skip highlighting on parse/syntax errors, just return what we have so far for this row.
            /// Rethrow all other exceptions (memory, resource, etc.) to avoid hiding real failures.
            if (e.code() != ErrorCodes::SYNTAX_ERROR)
                throw;
        }

        const auto expanded = expandHighlights(expected.highlights);

        for (const auto & range : expanded)
        {
            data_begin.push_back(range.begin - begin);
            data_end.push_back(range.end - begin);
            data_type.push_back(static_cast<Int8>(range.highlight));
            ++total;
        }
    }
};

}

REGISTER_FUNCTION(HighlightQuery)
{
    factory.registerFunction<FunctionQueryTokenization<HighlightQueryImpl>>(FunctionDocumentation{
        .description = R"(
Parses a ClickHouse SQL query string and returns an array of highlighted ranges for syntax highlighting.
Each range is a named tuple with the beginning position (in bytes), the end position, and the highlight type.
The highlight types describe the syntactic role of the fragment (keyword, identifier, function, etc.)
and can be used to assign colors in a UI. Inside LIKE and REGEXP string patterns, metacharacters
and escape characters are highlighted separately.
)",
        .syntax = "highlightQuery(query)",
        .arguments = {{"query", "A ClickHouse SQL query string. String."}},
        .returned_value = {"An array of named tuples `(begin UInt64, end UInt64, type Enum8(...))` representing highlighted ranges.", {"Array(Tuple(begin UInt64, end UInt64, type Enum8(...)))"}},
        .examples = {{"simple", "SELECT highlightQuery('SELECT 1')", R"([(0,6,'keyword'),(7,8,'number')])"}},
        .introduced_in = {26, 5},
        .category = FunctionDocumentation::Category::Other,
    });
}

}
