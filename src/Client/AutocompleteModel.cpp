#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>
#include <base/defines.h>

#include <Client/AutocompleteModel.h>
#include <Parsers/Lexer.h>
#include <Parsers/CommonParsers.h>


static std::string toUpperCaseString(const char * begin, const char * end)
{
    std::string result;

    result.reserve(end - begin);

    for (const char * ptr = begin; ptr != end; ++ptr)
    {
        result += static_cast<char>(std::toupper(static_cast<unsigned char>(*ptr)));
    }

    return result;
}

bool AutocompleteModel::isTokenIdentifier(const DB::Token & token) const
{
    if (token.type == DB::TokenType::QuotedIdentifier)
    {
        return true;
    }
    if (token.type != DB::TokenType::BareWord)
    {
        return false;
    }
    std::string token_content_uppercase = toUpperCaseString(token.begin, token.end);
    if (DB::getKeyWordSet().contains(token_content_uppercase))
    {
        return false;
    }
    return true;
}

bool AutocompleteModel::isTokenLiteral(const DB::Token & token) const
{
    return (
        token.type == DB::TokenType::StringLiteral || token.type == DB::TokenType::Number
        || toUpperCaseString(token.begin, token.end) == "NULL");
}

bool AutocompleteModel::isTokenOperator(const DB::Token & prev_token, const DB::Token & token) const
{
    if (token.type == DB::TokenType::Asterisk)
    {
        return isTokenIdentifier(prev_token) || isTokenLiteral(prev_token);
    }
    return operator_types.contains(token.type) || bare_words_operators.contains(toUpperCaseString(token.begin, token.end));
}

bool AutocompleteModel::isBadRec(const std::string & rec) const
{
    if (rec.empty() || rec == bos || rec == literal_placeholder)
        return true;
    /// Drop punctuation/operator-only predictions (e.g. "(", ")", ",", "="): they are noise as
    /// ghost text. Keep anything with an alphanumeric or non-ASCII byte: identifiers, keywords,
    /// functions, and quoted names, including ones made entirely of non-ASCII characters (any byte
    /// of a UTF-8 multi-byte sequence is >= 0x80, so a bytewise check suffices).
    return std::none_of(rec.begin(), rec.end(), [](unsigned char c) { return std::isalnum(c) != 0 || c >= 0x80; });
}

void AutocompleteModel::deleteDuplicatesKeepOrder(std::vector<std::string> & recs) const
{
    std::unordered_set<std::string> seen;
    auto it = recs.begin();

    while (it != recs.end())
    {
        if (seen.contains(*it))
        {
            it = recs.erase(it);
        }
        else
        {
            seen.insert(*it);
            ++it;
        }
    }
}

std::vector<std::string> AutocompleteModel::predictNextWords(DB::Lexer & lexer)
{
    /// Do not gate on whether any history was seeded: `clickhouse-local` and embedded clients have
    /// no persistent `system.query_log`, and a fresh session whose history load failed still learns
    /// from queries entered this session. `KneserNey::empty` guards the empty-model case below, so
    /// with no data we simply return no completions.
    auto tokens = preprocessTokens(lexer);

    if (tokens.empty() || markov.empty())
    {
        return {};
    }

    /// `addQuery` left-pads every training query with BOS markers, so the query-start n-grams the
    /// model learns are BOS-prefixed. Pad the prediction context the same way, otherwise the first
    /// `markov_order - 1` real tokens of a query (the common positions right after `SELECT`,
    /// `INSERT`, ...) could never match the full-order query-start statistics and would always back
    /// off to shorter contexts.
    for (size_t i = 1; i != markov_order; ++i)
    {
        tokens.insert(tokens.begin(), bos);
    }

    auto recs = markov.predictNext(tokens, recs_number);

    std::erase_if(recs, [this](const std::string & rec) { return isBadRec(rec); });
    deleteDuplicatesKeepOrder(recs);

    return recs;
}

void AutocompleteModel::addQuery(DB::Lexer & lexer)
{
    if (markov_order == 0)
    {
        return;
    }

    auto tokens = preprocessTokens(lexer);

    if (tokens.empty())
    {
        return;
    }

    for (size_t i = 1; i != markov_order; ++i)
    {
        tokens.insert(tokens.begin(), bos);
    }

    markov.addFullQuery(tokens);

    /// TODO: maybe increase only if the model has changed?
    markov.incTimestamp();
}

bool AutocompleteModel::isBareWordEqualToString(const DB::Token & token, const std::string & str) const
{
    return token.type == DB::TokenType::BareWord && toUpperCaseString(token.begin, token.end) == str;
}


void AutocompleteModel::squashTokens(
    std::vector<DB::Token> & tokens, size_t start_index, size_t end_index, const std::string & operator_literal) const
{
    auto it = bare_words_operators.find(operator_literal);
    if (it != bare_words_operators.end())
    {
        const char * begin_replacement = it->c_str();
        const char * end_replacement = begin_replacement + std::strlen(begin_replacement);
        DB::Token new_token(DB::TokenType::BareWord, begin_replacement, end_replacement);

        tokens.erase(tokens.begin() + start_index, tokens.begin() + end_index + 1);

        tokens.insert(tokens.begin() + start_index, new_token);
    }
}


void AutocompleteModel::squashOperatorTokens(std::vector<DB::Token> & tokens) const
{
    if (tokens.size() < 3)
    {
        return;
    }

    size_t initial_size = tokens.size();
    size_t replaced_2_cnt = 0;
    size_t replaced_3_cnt = 0;
    std::vector<std::string> after_not = {"BETWEEN", "IN", "LIKE", "EXISTS"};
    std::vector<std::string> before_not = {"AND", "OR"};

    for (size_t i = 1; i != tokens.size(); ++i)
    {
        if (i >= 2)
        {
            if (isBareWordEqualToString(tokens[i - 2], "GLOBAL") && isBareWordEqualToString(tokens[i - 1], "NOT")
                && isBareWordEqualToString(tokens[i], "IN"))
            {
                squashTokens(tokens, i - 2, i, "GLOBAL NOT IN");
                i -= 2;
                replaced_3_cnt++;
                continue;
            }
        }

        if (isBareWordEqualToString(tokens[i - 1], "GLOBAL") && isBareWordEqualToString(tokens[i], "IN"))
        {
            squashTokens(tokens, i - 1, i, "GLOBAL IN");
            i--;
            replaced_2_cnt++;
            continue;
        }

        /// After squashing we decrement `i` and must restart the outer loop, otherwise the checks
        /// below would read `tokens[i - 1]` with a possibly-zero `i` (e.g. an incomplete `NOT IN x `
        /// squashes the first two tokens, leaving `i == 0` and reading before the vector). A plain
        /// `continue` inside the inner `for` would only restart the inner loop, so use a flag.
        bool squashed = false;

        for (const auto & word : after_not)
        {
            if (isBareWordEqualToString(tokens[i - 1], "NOT") && isBareWordEqualToString(tokens[i], word))
            {
                std::string operator_literal = "NOT ";
                operator_literal += word;
                squashTokens(tokens, i - 1, i, operator_literal);
                i--;
                replaced_2_cnt++;
                squashed = true;
                break;
            }
        }

        if (squashed)
            continue;

        for (const auto & word : before_not)
        {
            if (isBareWordEqualToString(tokens[i - 1], word) && isBareWordEqualToString(tokens[i], "NOT"))
            {
                std::string operator_literal = word;
                operator_literal += " NOT";
                squashTokens(tokens, i - 1, i, operator_literal);
                i--;
                replaced_2_cnt++;
                squashed = true;
                break;
            }
        }

        if (squashed)
            continue;


        if (tokens[i - 1].type == DB::TokenType::Minus)
        {
            if (i >= 2 && !(isTokenIdentifier(tokens[i - 2]) || isTokenLiteral(tokens[i - 2])))
            {
                // If the current token is a Number, squash the Minus and Number tokens
                if (tokens[i].type == DB::TokenType::Number)
                {
                    const char * begin_replacement = tokens[i - 1].begin;
                    const char * end_replacement = tokens[i].end;
                    DB::Token new_token(DB::TokenType::Number, begin_replacement, end_replacement);

                    tokens.erase(tokens.begin() + i - 1, tokens.begin() + i + 1);

                    tokens.insert(tokens.begin() + i - 1, new_token);

                    i--;
                    replaced_2_cnt++;
                }
            }
        }
    }
    chassert(tokens.size() == initial_size - replaced_2_cnt - 2 * replaced_3_cnt);
}

std::vector<std::string> AutocompleteModel::tokensToStrings(const std::vector<DB::Token> & tokens) const
{
    std::vector<std::string> result;
    result.reserve(tokens.size());
    for (const auto & token : tokens)
    {
        /// String and numeric literals are normalized to a placeholder, for privacy first of all:
        /// the model is trained on raw query history, and replaying literal values as ghost text
        /// could leak emails, IDs, API keys, etc. typed in earlier sessions of the same user. It
        /// also pools the statistics of e.g. `LIMIT 10` and `LIMIT 100` into one n-gram. The
        /// placeholder is never offered as a prediction (see `isBadRec`). The `NULL` keyword-like
        /// literal is deliberately kept: it is not user data, and predicting it (e.g. after `IS`)
        /// is useful.
        if (token.type == DB::TokenType::StringLiteral || token.type == DB::TokenType::Number
            || token.type == DB::TokenType::HereDoc)
        {
            result.push_back(literal_placeholder);
        }
        else
        {
            /// Tokens are kept exactly as typed, keywords included. Canonicalizing keywords (e.g.
            /// upper-casing them) looks tempting, because it pools `select` and `SELECT` statistics,
            /// but a keyword can only be told from an identifier by parsing: `default` and `system`
            /// are common *identifiers* that collide with keyword names, and rewriting them made the
            /// model predict `DEFAULT` and `SYSTEM` after `FROM`. Since the history is the user's
            /// own, predictions in the user's own casing are consistent with what they type anyway.
            result.push_back(std::string(token.begin, token.end));
        }
    }
    chassert(result.size() == tokens.size());
    return result;
}


std::vector<std::string> AutocompleteModel::preprocessTokens(DB::Lexer & lexer) const
{
    std::vector<DB::Token> tokens_from_lexer{};

    while (true)
    {
        DB::Token token = lexer.nextToken();

        if (token.isEnd())
            break;

        if (token.isError())
            return {};

        if (!token.isSignificant())
            continue;

        tokens_from_lexer.push_back(token);
    }

    squashOperatorTokens(tokens_from_lexer);

    return tokensToStrings(tokens_from_lexer);
}


const std::string AutocompleteModel::bos = "<BOS>";

/// The angle brackets make a collision with a real token impossible: the lexer never produces them
/// as part of a bare word.
const std::string AutocompleteModel::literal_placeholder = "<LITERAL>";

const std::unordered_set<std::string> AutocompleteModel::bare_words_operators{
    "AND",
    "OR",
    "NOT",
    "AND NOT",
    "OR NOT",
    "IN",
    "NOT IN",
    "LIKE",
    "NOT LIKE",
    "BETWEEN",
    "NOT BETWEEN",
    "GLOBAL IN",
    "GLOBAL NOT IN",
    "EXISTS",
    "NOT EXISTS",
};

const std::unordered_set<DB::TokenType> AutocompleteModel::operator_types{
    // arithm
    DB::TokenType::Plus,
    DB::TokenType::Minus,
    DB::TokenType::Asterisk,
    DB::TokenType::Percent,

    // comparison
    DB::TokenType::Equals,
    DB::TokenType::NotEquals,
    DB::TokenType::GreaterOrEquals,
    DB::TokenType::LessOrEquals,
    DB::TokenType::Less,
    DB::TokenType::Greater,
    DB::TokenType::Spaceship,
};
