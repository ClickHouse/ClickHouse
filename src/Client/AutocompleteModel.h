#pragma once

#include <cstddef>
#include <utility>
#include <vector>
#include <span>
#include "TransformerModel.h"
#include "MarkovModel.h"
#include <Parsers/Lexer.h>
#include <unordered_set>

/// TODO: make const and static where possible

class AutocompleteModel {
private:
    /// TODO: construct markov models with given order
    size_t markov_order = 4;
    size_t processed_queries_cnt = 0;

    KneserNey markov_all = KneserNey();
    KneserNey markov_literals = KneserNey();
    KneserNey markov_identifiers = KneserNey();
    KneserNey markov_operators = KneserNey();

    GPTJModel transformer_model = GPTJModel("ggml-model-f32.bin");


    size_t recs_number = 4;


    // operators
    const std::unordered_set<std::string> bare_words_operators {
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

    const std::unordered_set<DB::TokenType> operator_types {
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
    

    static const std::unordered_set<std::string> keywords;

    bool isBareWordEqualToString(const DB::Token& token, const std::string& str);

    bool isTokenIdentifier(const DB::Token& token);
    bool isTokenKeyword(const DB::Token& token);
    bool isTokenLiteral(const DB::Token& token);
    bool isTokenOperator(const DB::Token& prev_token, const DB::Token& token);

    void squashTokens(std::vector<DB::Token>& tokens, size_t start_index, size_t end_index, const std::string& operator_literal);
    void squashOperatorTokens(std::vector<DB::Token>& tokens);
    void deleteDuplicatesKeepOrder(std::vector<std::string>& recs);
    

    std::vector<std::string> preprocessForTransformer(const std::vector<DB::Token>& tokens);
    std::vector<std::string> preprocessForMarkov(const std::vector<DB::Token>& tokens);

    bool isShortRec(const std::string& recomendation);
    bool isBadRec(const std::string& rec);

    std::vector<std::string> postprocessRecs(const std::vector<std::string>& raw_recs);

    // void insertOnFirstTwoPlaces(std::vector<std::string>& transformer_recs, const std::vector<std::string>& markov_predictions) {
    //     if (transformer_recs.size() == 1) {
    //         transformer_recs[0] = markov_predictions[0];
    //         return;
    //     }

    //     for (size_t i = transformer_recs.size() - 1; i > 0; --i) {
    //         transformer_recs[i] = transformer_recs[i - 1];
    //     }

    //     transformer_recs[0] = markov_predictions[0];
    //     transformer_recs[1] = markov_predictions[1];
    // }

    /// TODO: If Identifier/Literal on the first place maybe we want to show 2 suggestions from markov model? 
    void replaceWithMarkovPredictions(std::vector<std::string>& transformer_recs, std::span<const std::string> preprocessed_tokens_for_markov);



public:
    std::pair<std::vector<std::string>, std::vector<std::string>> preprocessTokens(DB::Lexer& lexer);

    std::vector<std::string> predictNextWords(DB::Lexer& lexer);

    void addQuery(DB::Lexer& lexer);
};
