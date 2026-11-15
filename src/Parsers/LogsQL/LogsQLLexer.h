#pragma once

#include <base/types.h>
#include <Common/Exception.h>

#include <string_view>
#include <vector>

namespace DB
{

/// The syntax error that the LogsQL lexer throws when the input ends inside a backtick-quoted
/// (raw, multiline) string: the query is unfinished rather than wrong, and the next lines can
/// complete it. (A double- or single-quoted string cannot span lines, so a newline could not
/// complete it, and it is an ordinary syntax error.) It is an ordinary `SYNTAX_ERROR` for everyone else; the interactive
/// client catches it by type to keep such a statement open for the next line
/// (see `ParserLogsQLQuery::isIncompleteAtEOF`).
class LogsQLUnexpectedEndOfInput : public Exception
{
public:
    using Exception::Exception;
};

/// Lexer for LogsQL - the query language of VictoriaLogs.
/// https://docs.victoriametrics.com/victorialogs/logsql/
///
/// It closely follows the lexer of the reference implementation
/// (lib/logstorage/parser.go in the VictoriaLogs repository, Apache 2.0):
///   - a token is a maximal run of "word" characters (ASCII alphanumeric, '_', and non-ASCII characters);
///   - strings are quoted with double quotes or backticks (Go syntax) or with single quotes;
///   - "=~", "!=", "!~" are two-character tokens, any other special character is a single-character token;
///   - '#' starts a comment which spans to the end of the line;
///   - adjacent (not separated by whitespace) tokens are glued into "compound tokens"
///     with the glue characters '+', '-', '/', ':', '.', '$' (see nextCompoundToken),
///     so that things like `foo-bar.com:1234/path` or `2025-07-20T10:20:30+03:00` are single tokens.
///
/// The only deviation from the reference implementation: any non-ASCII character is treated
/// as a word character, while VictoriaLogs treats only Unicode letters and digits as word characters.
class LogsQLLexer
{
public:
    /// `truncated` means that `end` was clipped by `max_query_size`, so running into it
    /// is reported as an exceeded query size rather than as an end of input.
    LogsQLLexer(const char * begin_, const char * end_, bool truncated_ = false);

    /// Advances to the next token.
    void nextToken();

    /// The decoded text of the current token (for quoted tokens - the unquoted and unescaped content).
    const String & getToken() const { noteInspection(); return token; }

    /// The raw text of the current token as it appears in the query.
    std::string_view getRawToken() const { noteInspection(); return raw_token; }

    /// The raw text of the previous token.
    const String & getPrevRawToken() const { return prev_raw_token; }

    /// Whether the current token was quoted. Quoted tokens are never keywords.
    bool isQuoted() const { noteInspection(); return quoted; }

    /// Whether there was whitespace (or a comment) between the previous and the current token.
    bool skippedSpace() const { noteInspection(); return skipped_space; }

    /// True if there are no more tokens.
    bool isEnd() const { noteInspection(); return isAtEndOfInput(); }

    /// True if the end of input was examined as the current token: some caller asked whether it
    /// is a keyword, a trailer, the end and so on, since the lexer last moved to it. A syntax error
    /// thrown in this state is about the input missing at the end, e.g. after `error and`, while
    /// one thrown without it is about tokens that have already been read, e.g. the argument count
    /// of a complete `re(a,b)`, even though the lexer has reached the end of input by then.
    /// Reading this does not count as an examination itself.
    bool hasExaminedEnd() const { return examined_end; }

    /// Case-insensitive comparison of the current token with the given keywords.
    /// Quoted tokens never match keywords.
    bool isKeyword(std::string_view keyword) const;
    bool isKeywordAny(const std::vector<std::string_view> & keywords) const;

    /// True if the current token finishes a query part: "|", ")", ";" or end of input.
    bool isQueryPartTrailer() const;

    /// Reads a compound token: the current token glued with the following adjacent tokens.
    /// Tokens from `stop_tokens` are not glued. Throws if the current token cannot start a compound token.
    String nextCompoundToken(const std::vector<std::string_view> & stop_tokens = {});

    /// The start of the raw text of the current token. Used to determine the consumed part of the query.
    const char * getTokenBegin() const { return token_begin; }

    /// Last characters of the query before the current position, for error messages.
    String context() const;

    /// Throws SYNTAX_ERROR with the current context appended to the message.
    [[noreturn]] void throwSyntaxError(const String & message) const;

    /// Like `throwSyntaxError`, for an input that ends where more of it is required and can
    /// follow on the next line, i.e. inside a backtick-quoted string. Throws `LogsQLUnexpectedEndOfInput`.
    [[noreturn]] void throwUnexpectedEndOfInput(const String & message) const;

    struct State
    {
        const char * current;
        const char * token_begin;
        String token;
        std::string_view raw_token;
        String prev_raw_token;
        bool quoted;
        bool skipped_space;
    };

    State backupState() const;
    void restoreState(const State & state);

    /// Checks that if the previous token is adjacent (no whitespace in between), it is one of the allowed ones.
    /// Otherwise throws a "missing whitespace" error. Mirrors lexer.checkPrevAdjacentToken from VictoriaLogs.
    void checkPrevAdjacentToken(const std::vector<std::string_view> & allowed) const;

    static bool isWord(std::string_view text);

    /// Throws if the (possibly clipped) end of input was reached because of `max_query_size`.
    void checkTruncation(const char * pos) const;

private:
    const char * begin;
    const char * end;
    bool truncated;

    /// Position right after the current token.
    const char * current;
    const char * token_begin;

    String token;
    std::string_view raw_token;
    String prev_raw_token;
    bool quoted = false;
    bool skipped_space = false;

    /// Set by `noteInspection` and cleared when the lexer moves (see `hasExaminedEnd`).
    mutable bool examined_end = false;

    bool isAtEndOfInput() const { return raw_token.empty() && current == end; }

    /// Called by the accessors of the current token.
    void noteInspection() const
    {
        if (isAtEndOfInput())
            examined_end = true;
    }

    bool isAllowedCompoundToken(const std::vector<std::string_view> & stop_tokens) const;

    String decodeDoubleQuoted(const char *& pos) const;
    String decodeBacktickQuoted(const char *& pos) const;
    String decodeSingleQuoted(const char *& pos) const;
};

}
