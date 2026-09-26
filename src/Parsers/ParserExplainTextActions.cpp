#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <Parsers/ParserExplainTextActions.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/TokenIterator.h>

#include <Parsers/ASTExplainTextAction.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

#include <algorithm>
#include <memory>
#include <string_view>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{
class ParserExplainTextAction final : public IParserBase
{
protected:
    const char * getName() const override { return "EXPLAIN TEXT action"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

bool ParserExplainTextAction::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword oneline_p(Keyword::ONELINE);
    ParserKeyword multiline_p(Keyword::MULTILINE);
    ParserKeyword modify_p(Keyword::MODIFY);
    ParserKeyword limit_p(Keyword::LIMIT);
    ParserKeyword offset_p(Keyword::OFFSET);
    ParserKeyword page_p(Keyword::PAGE);
    ParserKeyword format_p(Keyword::FORMAT);

    ASTExplainTextAction::Kind kind = ASTExplainTextAction::Kind::Oneline;
    ASTPtr operand;

    if (oneline_p.ignore(pos, expected))
    {
        kind = ASTExplainTextAction::Kind::Oneline;
    }
    else if (multiline_p.ignore(pos, expected))
    {
        kind = ASTExplainTextAction::Kind::Multiline;
    }
    else if (page_p.ignore(pos, expected))
    {
        kind = ASTExplainTextAction::Kind::Page;

        ParserUnsignedInteger page_number_p;
        if (!page_number_p.parse(pos, operand, expected))
            return false;
    }
    else if (modify_p.ignore(pos, expected))
    {
        if (limit_p.ignore(pos, expected))
        {
            kind = ASTExplainTextAction::Kind::ModifyLimit;

            ParserExpression expression_p;
            if (!expression_p.parse(pos, operand, expected))
                return false;
        }
        else if (offset_p.ignore(pos, expected))
        {
            kind = ASTExplainTextAction::Kind::ModifyOffset;

            ParserExpression expression_p;
            if (!expression_p.parse(pos, operand, expected))
                return false;
        }
        else if (format_p.ignore(pos, expected))
        {
            kind = ASTExplainTextAction::Kind::ModifyFormat;

            ParserIdentifier identifier_p;
            if (!identifier_p.parse(pos, operand, expected))
                return false;
        }
        else
        {
            return false;
        }
    }
    else
    {
        return false;
    }

    auto action = make_intrusive<ASTExplainTextAction>(kind);
    if (operand)
        action->setOperand(std::move(operand));

    node = std::move(action);
    return true;
}

bool tokenEqualsKeyword(const Token & token, Keyword keyword)
{
    return token.type == TokenType::BareWord && equalsCaseInsensitive(
                                    std::string_view(token.begin, token.size()),
                                    toStringView(keyword));
}
}
bool canFollowExplainTextActions(const Token & token)
{
    if (token.type == TokenType::EndOfStream
        || token.type == TokenType::Semicolon
        || token.type == TokenType::VerticalDelimiter
        || token.type == TokenType::ClosingRoundBracket)
    {
        return true;
    }

    if (token.type != TokenType::BareWord)
        return false;
    const std::string_view text(token.begin, token.size());

    return equalsCaseInsensitive(text, toStringView(Keyword::FORMAT))
        || equalsCaseInsensitive(text, toStringView(Keyword::SETTINGS))
        || equalsCaseInsensitive(text, "INTO");
}

bool isExplainTextActionLeadingToken(const Token & token)
{
    return tokenEqualsKeyword(token, Keyword::MODIFY)
        || tokenEqualsKeyword(token, Keyword::PAGE)
        || tokenEqualsKeyword(token, Keyword::ONELINE)
        || tokenEqualsKeyword(token, Keyword::MULTILINE);
}

bool ParserExplainTextActions::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserList actions_p(
        std::make_unique<ParserExplainTextAction>(),
        std::make_unique<ParserToken>(TokenType::Comma),
        false);

    return actions_p.parse(pos, node, expected);
}

bool parseExplainTextBareSourceAndActions(IParser::Pos & pos, ASTPtr & query, ASTPtr & actions, Expected & expected, const char * end, bool allow_settings_after_format_in_insert)
{
    query = nullptr;
    actions = nullptr;

    auto source_begin = pos;
    std::vector<IParser::Pos> candidates;

    size_t round_depth{0};
    size_t square_depth{0};
    size_t curly_depth{0};
    TokenType previous_type{TokenType::EndOfStream};

    auto scan = pos;
    while (!scan->isEnd() && !scan->isError())
    {
        const bool at_top_level = round_depth == 0 && square_depth == 0 && curly_depth == 0;

        if (at_top_level && (scan->type == TokenType::Semicolon || scan->type == TokenType::VerticalDelimiter || scan->type == TokenType::ClosingRoundBracket))
            break;

        if (at_top_level && previous_type != TokenType::Comma && isExplainTextActionLeadingToken(*scan))
            candidates.push_back(scan);

        switch (scan->type)
        {
            case TokenType::OpeningRoundBracket:
                ++round_depth;
                break;
            case TokenType::ClosingRoundBracket:
                if (round_depth != 0)
                    --round_depth;
                break;
            case TokenType::OpeningSquareBracket:
                ++square_depth;
                break;
            case TokenType::ClosingSquareBracket:
                if (square_depth != 0)
                    --square_depth;
                break;
            case TokenType::OpeningCurlyBrace:
                ++curly_depth;
                break;
            case TokenType::ClosingCurlyBrace:
                if (curly_depth != 0)
                    --curly_depth;
                break;
            default:
                break;
        }

        previous_type = scan->type;
        ++scan;
    }

    ParserExplainTextActions actions_parser;

    for (auto candidate : candidates)
    {
        candidate.backtracks = pos.backtracks;
        pos = candidate;

        ASTPtr candidate_actions;
        const bool parsed_actions = actions_parser.parse(pos, candidate_actions, expected);
        const bool valid_actions_end = parsed_actions && canFollowExplainTextActions(*pos);
        const bool missing_comma = parsed_actions && isExplainTextActionLeadingToken(*pos);
        auto actions_end = pos;

        /// Return to the source beginning and charge the candidate
        /// against the shared parser backtrack budget.
        auto rewind = source_begin;
        rewind.backtracks = pos.backtracks;
        pos = rewind;

        if (!valid_actions_end && !missing_comma)
            continue;

        const char * prefix_end = candidate->begin;
        Tokens prefix_tokens(source_begin->begin, prefix_end);
        IParser::Pos prefix_pos(prefix_tokens, pos);

        ParserQuery source_parser(prefix_end, allow_settings_after_format_in_insert);

        ASTPtr candidate_query;
        const bool parsed_query = source_parser.parse(prefix_pos, candidate_query, expected)
                                && prefix_pos->type == TokenType::EndOfStream;

        pos.backtracks = std::max(pos.backtracks, prefix_pos.backtracks);

        if (!parsed_query)
            continue;

        /// confirm a complete source prefix then diagnose a missing separator
        if (missing_comma)
        {
            const auto & unexpected_token = *actions_end;
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing comma between EXPLAIN TEXT actions before '{}'", std::string_view(unexpected_token.begin, unexpected_token.size()));
        }

        auto committed_end = actions_end;
        committed_end.backtracks = std::max(committed_end.backtracks, pos.backtracks);
        pos = committed_end;

        query = std::move(candidate_query);
        actions = std::move(candidate_actions);
        return true;
    }

    /// A `SETTINGS` clause after the trailing `FORMAT` of an `INSERT ... SELECT` belongs to
    /// `EXPLAIN TEXT` like the `FORMAT` itself, whatever `allow_settings_after_format_in_insert`
    /// says, so the insert parser must not consume it here; the outer `ParserQueryWithOutput` will.
    ParserQuery source_parser(end, /*allow_settings_after_format_in_insert=*/ false, /*implicit_select=*/ false, /*parse_output_options=*/ false);
    if (!source_parser.parse(pos, query, expected))
        return false;

    /// `ParserQueryWithOutput` is disabled above so that a trailing `FORMAT` or `INTO OUTFILE` stays
    /// with `EXPLAIN TEXT`. A `SETTINGS` clause directly after the statement belongs to the statement,
    /// as it does for a `SELECT` (whose own parser takes it) and whenever actions follow, so attach it
    /// to the source the way `ParserQueryWithOutput` would have. A nested `EXPLAIN` over
    /// `INSERT ... SELECT` may already have taken the trailing `FORMAT` onto its own node; a `SETTINGS`
    /// clause after it follows that `FORMAT` and belongs to `EXPLAIN TEXT` together with it.
    if (auto * query_with_output = outputOptionsOwner(query.get());
        query_with_output && !query_with_output->settings_ast && !query_with_output->format_ast)
    {
        auto saved = pos;
        ParserKeyword settings_keyword(Keyword::SETTINGS);
        ASTPtr settings;
        if (settings_keyword.ignore(pos, expected) && ParserSetQuery(true).parse(pos, settings, expected))
            query_with_output->set(query_with_output->settings_ast, std::move(settings));
        else
            pos = saved;
    }

    return true;
}
}
