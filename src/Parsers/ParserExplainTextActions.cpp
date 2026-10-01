#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <Parsers/ParserExplainTextActions.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/TokenIterator.h>

#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTExplainTextAction.h>
#include <Parsers/ASTParallelWithQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

#include <algorithm>
#include <memory>
#include <string_view>
#include <vector>

#if !defined(CLICKHOUSE_PARSER_NO_DCL)
#include <Parsers/Access/ASTExecuteAsQuery.h>
#endif

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

/// Whether an action list written after the query `node` could also belong to a nested `EXPLAIN TEXT`
/// that ends the text of `node` reached through the subquery of `EXECUTE AS`, the last statement of
/// `PARALLEL WITH`, the query of `CREATE HANDLER` or `ALTER HANDLER`, or the query of another `EXPLAIN`.
/// Such an `EXPLAIN TEXT` could take the actions unless it already has its own.
bool endsWithExplainTextThatCanTakeActions(const IAST * node)
{
    while (node)
    {
        if (const auto * explain = node->as<ASTExplainQuery>())
        {
            /// Output options after the source of a nested `EXPLAIN TEXT` that takes no actions are
            /// its own, but the same text followed by actions would give them to its source instead.
            if (explain->getKind() == ASTExplainQuery::FormattedQuery)
                return !explain->getActions();

            /// Output options of another `EXPLAIN` follow all the text of its explained query, so a
            /// nested `EXPLAIN TEXT` cannot take actions written after them.
            if (explain->hasOutputOptions())
                return false;

            node = explain->getExplainedQuery().get();
            continue;
        }
#if !defined(CLICKHOUSE_PARSER_NO_DCL)
        /// Output options of `EXECUTE AS` are hoisted from the end of its subquery.
        if (const auto * execute_as = node->as<ASTExecuteAsQuery>())
        {
            node = execute_as->subquery.get();
            continue;
        }
#endif

        /// `AS <query>` is the last clause of a `CREATE HANDLER` and `ALTER HANDLER`.
        if (const auto * handler = node->as<ASTCreateHandlerQuery>())
        {
            node = handler->query.get();
            continue;
        }

        if (const auto * parallel = node->as<ASTParallelWithQuery>())
        {
            if (parallel->children.empty())
                return false;

            node = parallel->children.back().get();
            continue;
        }
        return false;
    }
    return false;
}

/// Keep what `from` expected at its rightmost position, as if it had been collected in `to`.
void addExpectedVariants(Expected & to, const Expected & from)
{
    for (const auto * variant : from.variants)
        to.add(from.max_parsed_pos, variant);
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

    /// The scan and the rejected action lists below only look for where the source ends. A syntax
    /// error is reported at the rightmost token read, so each lookahead restores it. Otherwise every
    /// error in the source would be reported at the end of the statement. The lookahead reads the
    /// token stream being parsed, which `end` does not always bound. The debug check in
    /// `executeQuery` parses the formatted query back with the parser built for the original one.
    const size_t max_pos_before_lookahead = pos.getMaxPos();
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

    pos.restoreMaxPos(max_pos_before_lookahead);

    ParserExplainTextActions actions_parser;

    /// What the rejected candidates expected at their rightmost position. The error is reported
    /// where the whole-source parse below stops, so this is added only if that is the same token.
    Expected rejected_expected;

    for (auto candidate : candidates)
    {
        candidate.backtracks = pos.backtracks;
        pos = candidate;

        /// Collected apart from `expected`. A rejected candidate is a reading of the statement that
        /// is not taken, so its highlights would color tokens wrongly and its variants may not
        /// describe the token the error is reported at.
        Expected candidate_expected;
        candidate_expected.enable_highlighting = expected.enable_highlighting;

        ASTPtr candidate_actions;
        const bool parsed_actions = actions_parser.parse(pos, candidate_actions, candidate_expected);
        const bool valid_actions_end = parsed_actions && canFollowExplainTextActions(*pos);
        const bool missing_comma = parsed_actions && isExplainTextActionLeadingToken(*pos);
        auto actions_end = pos;

        /// Return to the source beginning and charge the candidate
        /// against the shared parser backtrack budget.
        auto rewind = source_begin;
        rewind.backtracks = pos.backtracks;
        pos = rewind;

        ASTPtr candidate_query;
        bool parsed_query = false;
        if (valid_actions_end || missing_comma)
        {
            const char * prefix_end = candidate->begin;
            Tokens prefix_tokens(source_begin->begin, prefix_end);
            IParser::Pos prefix_pos(prefix_tokens, pos);
            ParserQuery source_parser(prefix_end, allow_settings_after_format_in_insert);

            parsed_query = source_parser.parse(prefix_pos, candidate_query, candidate_expected)
                        && prefix_pos->type == TokenType::EndOfStream;

            pos.backtracks = std::max(pos.backtracks, prefix_pos.backtracks);
        }

        if (!parsed_query)
        {
            pos.restoreMaxPos(max_pos_before_lookahead);
            addExpectedVariants(rejected_expected, candidate_expected);
            continue;
        }

        if (endsWithExplainTextThatCanTakeActions(candidate_query.get()))
            throw Exception(ErrorCodes::SYNTAX_ERROR,
                "The EXPLAIN TEXT actions starting at '{}' could belong to the nested EXPLAIN TEXT or to the enclosing one. "
                "Put the source of the enclosing EXPLAIN TEXT in parentheses: "
                "EXPLAIN TEXT (EXPLAIN TEXT SELECT 1 ONELINE) applies them to the nested one, "
                "EXPLAIN TEXT (EXPLAIN TEXT SELECT 1) ONELINE to the enclosing one",
                std::string_view(candidate->begin, candidate->size()));

        /// confirm a complete source prefix then diagnose a missing separator
        if (missing_comma)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Missing comma between EXPLAIN TEXT actions before '{}'", std::string_view(actions_end->begin, actions_end->size()));

        /// the action list and the token after it stay read. The statement is parsed that way
        addExpectedVariants(expected, candidate_expected);
        for (const auto & range : candidate_expected.highlights)
            expected.highlight(range);

        pos = actions_end;

        query = std::move(candidate_query);
        actions = std::move(candidate_actions);
        return true;
    }

    /// A `SETTINGS` clause after the trailing `FORMAT` of an `INSERT ... SELECT` belongs to
    /// `EXPLAIN TEXT` like the `FORMAT` itself, whatever `allow_settings_after_format_in_insert`
    /// says, so the insert parser must not consume it here; the outer `ParserQueryWithOutput` will.
    ParserQuery source_parser(end, /*allow_settings_after_format_in_insert=*/ false, /*implicit_select=*/ false, /*parse_output_options=*/ false);

    const bool parsed_source = source_parser.parse(pos, query, expected);
    /// A rejected action list can describe the token the statement fails at, as `unsigned integer`
    /// after `PAGE`, which the whole-source reading does not mention.
    if (rejected_expected.max_parsed_pos && rejected_expected.max_parsed_pos == expected.max_parsed_pos)
        addExpectedVariants(expected, rejected_expected);

    if (!parsed_source)
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
