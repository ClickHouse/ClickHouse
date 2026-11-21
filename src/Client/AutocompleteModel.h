#pragma once

#include <cstddef>
#include <string>
#include <unordered_set>
#include <vector>
#include <Parsers/Lexer.h>
#include <Client/MarkovModel.h>


/// Predicts the next SQL tokens from a Kneser-Ney n-gram (Markov) model seeded from the user's
/// query history and updated with every query entered during the session. The model works on a
/// lightly normalized token stream: multi-word operators (`NOT IN`, `GLOBAL NOT IN`, ...) are
/// squashed into single tokens and string/numeric literals are replaced with a placeholder (so
/// that literal values from the query history are never replayed as predictions, and so that
/// queries differing only in literals share their statistics).
///
/// This is the Markov-only core. A small transformer that predicts the *type* of the next token
/// (literal / identifier / operator / keyword) to route per-type Markov models can be layered on
/// top in a follow-up without changing this interface.
class AutocompleteModel
{
private:
    /// TODO: construct the Markov model with a configurable order.
    size_t markov_order = 4;

    KneserNey markov = KneserNey(markov_order);

    /// How many next-token candidates to return, in probability order.
    size_t recs_number = 4;

    /// Left-padding marker so a query's first real tokens still have a full-order context.
    static const std::string bos;

    /// Stand-in for string/numeric literal tokens (see `tokensToStrings`).
    static const std::string literal_placeholder;

    const static std::unordered_set<std::string> bare_words_operators;
    const static std::unordered_set<DB::TokenType> operator_types;

    bool isBareWordEqualToString(const DB::Token & token, const std::string & str) const;

    bool isTokenIdentifier(const DB::Token & token) const;
    bool isTokenLiteral(const DB::Token & token) const;
    bool isTokenOperator(const DB::Token & prev_token, const DB::Token & token) const;

    void squashTokens(std::vector<DB::Token> & tokens, size_t start_index, size_t end_index, const std::string & operator_literal) const;
    void squashOperatorTokens(std::vector<DB::Token> & tokens) const;
    void deleteDuplicatesKeepOrder(std::vector<std::string> & recs) const;

    std::vector<std::string> tokensToStrings(const std::vector<DB::Token> & tokens) const;

    /// Drop candidates that must not or cannot usefully be shown: the padding marker, the literal
    /// placeholder, empty strings, and punctuation-only tokens.
    bool isBadRec(const std::string & rec) const;

public:
    /// Lex `lexer` into the normalized token-string stream the Markov model consumes.
    std::vector<std::string> preprocessTokens(DB::Lexer & lexer) const;

    /// Predict up to `recs_number` next tokens for the (partial) query in `lexer`, most likely first.
    std::vector<std::string> predictNextWords(DB::Lexer & lexer);

    /// Feed a completed query into the model so subsequent predictions reflect it.
    void addQuery(DB::Lexer & lexer);
};
