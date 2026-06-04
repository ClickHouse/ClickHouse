#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFromJSON.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/Lexer.h>
#include <Poco/String.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_ast_depth;
    extern const SettingsUInt64 max_ast_elements;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Tokenize a query string, returning all tokens (including whitespace and comments).
std::vector<Token> tokenize(const String & query) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    std::vector<Token> tokens; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    Lexer lexer(query.data(), query.data() + query.size());
    while (true)
    {
        auto token = lexer.nextToken();
        tokens.push_back(token);
        if (token.isEnd() || token.isError())
            break;
    }
    return tokens;
}

/// Given a canonical formatted query and the original query text,
/// produce a version that preserves comments, whitespace, and indentation
/// from the original where possible.
///
/// Algorithm:
/// 1. Tokenize both the original and canonical queries.
/// 2. Extract the significant (non-whitespace, non-comment) tokens from each.
/// 3. Walk through both token sequences. Where significant tokens match,
///    use the original inter-token material (whitespace + comments).
///    Where they diverge, fall back to canonical formatting.
String formatWithOriginalWhitespace(const String & canonical, const String & original)
{
    auto orig_tokens = tokenize(original);
    auto canon_tokens = tokenize(canonical);

    /// Collect significant tokens with their indices in the full token stream.
    struct SignificantToken
    {
        std::string_view text;
        TokenType type;
        size_t index; /// Index in the full token array.
    };

    auto extractSignificant = [](const std::vector<Token> & tokens) -> std::vector<SignificantToken> /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        std::vector<SignificantToken> result; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        for (size_t i = 0; i < tokens.size(); ++i)
            if (tokens[i].isSignificant() && !tokens[i].isEnd())
                result.push_back({std::string_view(tokens[i].begin, tokens[i].size()), tokens[i].type, i});
        return result;
    };

    auto orig_sig = extractSignificant(orig_tokens);
    auto canon_sig = extractSignificant(canon_tokens);

    /// Get the inter-token material (whitespace + comments) between token at full_index
    /// and the previous significant token (or start of string).
    auto getInterTokenMaterial = [](const std::vector<Token> & tokens, size_t sig_idx, const String & source) -> std::string_view /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        if (sig_idx == 0)
        {
            /// Material before the first significant token.
            const char * start = source.data();
            const char * end = tokens.empty() ? source.data() : tokens[0].begin;
            /// Find the actual first significant token's begin.
            for (const auto & token : tokens)
            {
                if (token.isSignificant() && !token.isEnd())
                {
                    end = token.begin;
                    break;
                }
            }
            return {start, static_cast<size_t>(end - start)};
        }

        /// Find the full-array index of the previous significant token.
        size_t prev_full_idx = 0;
        size_t count = 0;
        for (size_t i = 0; i < tokens.size(); ++i)
        {
            if (tokens[i].isSignificant() && !tokens[i].isEnd())
            {
                if (count == sig_idx - 1)
                {
                    prev_full_idx = i;
                    break;
                }
                ++count;
            }
        }

        /// Find the full-array index of the current significant token.
        size_t cur_full_idx = 0;
        count = 0;
        for (size_t i = 0; i < tokens.size(); ++i)
        {
            if (tokens[i].isSignificant() && !tokens[i].isEnd())
            {
                if (count == sig_idx)
                {
                    cur_full_idx = i;
                    break;
                }
                ++count;
            }
        }

        const char * start = tokens[prev_full_idx].end;
        const char * end = tokens[cur_full_idx].begin;
        return {start, static_cast<size_t>(end - start)};
    };

    /// Walk both sequences in lockstep, using original formatting where tokens match.
    String result;
    size_t ci = 0; /// Index into canon_sig.
    size_t oi = 0; /// Index into orig_sig.

    /// Check if two tokens are equivalent.
    /// Case-insensitive only for SQL keywords. `BareWord` covers both keywords and
    /// identifiers at the lexer level, so we explicitly look up the uppercase form
    /// in the parser's keyword set to distinguish them. Identifiers, string literals,
    /// quoted identifiers, and other tokens must match exactly to preserve semantics
    /// for case-sensitive identifier names.
    const auto & keyword_set = getKeyWordSet();
    auto isKeyword = [&keyword_set](const SignificantToken & t) -> bool
    {
        if (t.type != TokenType::BareWord)
            return false;
        String upper(t.text);
        Poco::toUpperInPlace(upper);
        return keyword_set.contains(upper);
    };

    auto tokensMatch = [&isKeyword](const SignificantToken & a, const SignificantToken & b) -> bool
    {
        if (a.text.size() != b.text.size())
            return false;

        bool case_insensitive = isKeyword(a) && isKeyword(b);

        for (size_t i = 0; i < a.text.size(); ++i)
        {
            if (a.text[i] == b.text[i])
                continue;
            if (!case_insensitive)
                return false;
            /// Case-insensitive comparison for ASCII.
            char la = (a.text[i] >= 'A' && a.text[i] <= 'Z') ? static_cast<char>(a.text[i] + 32) : a.text[i];
            char lb = (b.text[i] >= 'A' && b.text[i] <= 'Z') ? static_cast<char>(b.text[i] + 32) : b.text[i];
            if (la != lb)
                return false;
        }
        return true;
    };

    while (ci < canon_sig.size())
    {
        if (oi < orig_sig.size() && tokensMatch(canon_sig[ci], orig_sig[oi]))
        {
            /// Tokens match — use original inter-token material and original token text.
            result += getInterTokenMaterial(orig_tokens, oi, original);
            result += orig_sig[oi].text;
            ++ci;
            ++oi;
        }
        else
        {
            /// Tokens diverge — use canonical formatting from here.
            /// Output the rest of the canonical query from this token onwards.
            const char * start = (ci == 0)
                ? canonical.data()
                : canon_sig[ci - 1].text.data() + canon_sig[ci - 1].text.size();

            /// Find the next point where tokens re-align.
            /// Simple heuristic: skip one canonical token and try to re-sync.
            size_t next_ci = ci + 1;
            size_t next_oi = oi;
            bool resync = false;

            /// Try to find re-alignment within a small window.
            for (size_t look = 0; look < 20 && next_ci + look < canon_sig.size(); ++look)
            {
                for (size_t olook = 0; olook < 20 && next_oi + olook < orig_sig.size(); ++olook)
                {
                    if (tokensMatch(canon_sig[next_ci + look], orig_sig[next_oi + olook]))
                    {
                        /// Found re-alignment. Output canonical text for the divergent tokens up to
                        /// (but not including) the inter-token material before the resync token —
                        /// that whitespace will be re-emitted from the original on the next iteration.
                        const auto & last_canon = canon_sig[next_ci + look - 1];
                        const char * end = last_canon.text.data() + last_canon.text.size();
                        if (end > start)
                            result += std::string_view(start, static_cast<size_t>(end - start));
                        ci = next_ci + look;
                        oi = next_oi + olook;
                        resync = true;
                        goto done_resync;
                    }
                }
            }
            done_resync:

            if (!resync)
            {
                /// Could not re-sync. Output rest of canonical query.
                const char * end = canonical.data() + canonical.size();
                result += std::string_view(start, static_cast<size_t>(end - start));
                return result;
            }
        }
    }

    /// Append any trailing material from the original (trailing comments/whitespace).
    if (oi > 0 && oi <= orig_sig.size())
    {
        auto last_orig = orig_sig[oi - 1];
        const char * after_last = last_orig.text.data() + last_orig.text.size();
        const char * end = original.data() + original.size();
        if (after_last < end)
            result += std::string_view(after_last, static_cast<size_t>(end - after_last));
    }

    return result;
}


/// formatQueryFromJSON(json_string) -> SQL query string
/// formatQueryFromJSON(json_string, original_query) -> SQL with preserved formatting
class FunctionFormatQueryFromJSON : public IFunction
{
public:
    static constexpr auto name = "formatQueryFromJSON";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionFormatQueryFromJSON>(context);
    }

    explicit FunctionFormatQueryFromJSON(ContextPtr context)
    {
        const Settings & settings = context->getSettingsRef();
        max_ast_depth = settings[Setting::max_ast_depth];
        max_ast_elements = settings[Setting::max_ast_elements];
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes 1 or 2 arguments, got {}",
                getName(), arguments.size());

        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be String, got {}",
                getName(), arguments[0]->getName());

        if (arguments.size() == 2 && !isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of function {} must be String, got {}",
                getName(), arguments[1]->getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result = ColumnString::create();

        const auto * json_col = arguments[0].column.get();
        const auto * orig_col = arguments.size() > 1 ? arguments[1].column.get() : nullptr;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto json = String(json_col->getDataAt(i));
            auto ast = IAST::createFromJSON(json, max_ast_depth, max_ast_elements);

            /// Defense in depth: also walk the constructed AST and verify its
            /// post-build size/depth, mirroring `checkASTSizeLimits` in `executeQuery`.
            if (max_ast_depth)
                ast->checkDepth(max_ast_depth);
            if (max_ast_elements)
                ast->checkSize(max_ast_elements);

            WriteBufferFromOwnString buf;
            if (orig_col)
            {
                /// Two-argument form: format with preserved whitespace/comments.
                IAST::FormatSettings format_settings(/*one_line=*/true);
                ast->format(buf, format_settings);
                String canonical = buf.str();

                auto original = String(orig_col->getDataAt(i));
                result->insert(formatWithOriginalWhitespace(canonical, original));
            }
            else
            {
                /// One-argument form: canonical formatting.
                IAST::FormatSettings format_settings(/*one_line=*/true);
                ast->format(buf, format_settings);
                result->insert(buf.str());
            }
        }

        return result;
    }

private:
    size_t max_ast_depth;
    size_t max_ast_elements;
};

}


REGISTER_FUNCTION(FormatQueryFromJSON)
{
    FunctionDocumentation::Description description = R"(
Takes a JSON representation of a SQL AST (as produced by `parseQueryToJSON`) and formats it back into a SQL query string.

With one argument, produces canonically formatted SQL.
With two arguments `(json, original_query)`, preserves comments, whitespace, and indentation from the original query on a best-effort basis.

The deserialized AST is bounded by the current session's `max_ast_depth` and `max_ast_elements` settings.
Together with `parseQueryToJSON`, this function enables programmatic inspection and transformation of queries
through their JSON AST form.
    )";
    FunctionDocumentation::Syntax syntax = "formatQueryFromJSON(json[, original_query])";
    FunctionDocumentation::Arguments func_arguments = {
        {"json", "A JSON string representing a SQL AST.", {"String"}},
        {"original_query", "Optional. The original SQL query to preserve formatting from.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"A SQL query string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Round-trip",
        R"(SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t WHERE x > 1'));)",
        R"(
┌─formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t WHERE x > 1'))─┐
│ SELECT a, b FROM t WHERE x > 1                                         │
└─────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Preserve formatting",
        R"(SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t'), 'SELECT /* comment */ a FROM t');)",
        R"(
┌─formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t'), 'SELECT /* comment */ a FROM t')─┐
│ SELECT /* comment */ a FROM t                                                             │
└───────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, func_arguments, {}, returned_value, examples, {26, 4}, category};

    factory.registerFunction<FunctionFormatQueryFromJSON>(documentation);
}

}
