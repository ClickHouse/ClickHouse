#pragma once

#include <cstddef>
#include <utility>
#include <vector>
#include <span>
#include "TransformerModel.h"
#include "MarkovModel.h"
#include <Parsers/Lexer.h>
#include <unordered_set>

class AutocompleteModel {
private:
    /// TODO: construct markov models with given order
    size_t markov_order = 4;
    size_t processed_queries_cnt = 0;

    KneserNey markov_all = KneserNey(markov_order);
    KneserNey markov_literals = KneserNey(markov_order);
    KneserNey markov_identifiers = KneserNey(markov_order);
    KneserNey markov_operators = KneserNey(markov_order);

    GPTJModel transformer_model = GPTJModel("ggml-model-f32.bin");


    size_t recs_number = 4;


    const static std::unordered_set<std::string> bare_words_operators;
    const static std::unordered_set<DB::TokenType> operator_types;
    const static std::unordered_set<std::string> short_tokens;
    static const std::unordered_set<std::string> keywords;

    bool isBareWordEqualToString(const DB::Token& token, const std::string& str) const;

    bool isTokenIdentifier(const DB::Token& token) const;
    bool isTokenKeyword(const DB::Token& token) const;
    bool isTokenLiteral(const DB::Token& token) const;
    bool isTokenOperator(const DB::Token& prev_token, const DB::Token& token) const;

    void squashTokens(std::vector<DB::Token>& tokens, size_t start_index, size_t end_index, const std::string& operator_literal) const;
    void squashOperatorTokens(std::vector<DB::Token>& tokens) const;
    void deleteDuplicatesKeepOrder(std::vector<std::string>& recs) const;
    

    std::vector<std::string> preprocessForTransformer(const std::vector<DB::Token>& tokens) const;
    std::vector<std::string> preprocessForMarkov(const std::vector<DB::Token>& tokens) const;

    bool isShortRec(const std::string& recomendation) const;
    bool isBadRec(const std::string& rec) const;

    std::vector<std::string> postprocessRecs(const std::vector<std::string>& raw_recs) const;
    void replaceWithMarkovPredictions(std::vector<std::string>& transformer_recs, std::span<const std::string> preprocessed_tokens_for_markov) const;



public:
    std::pair<std::vector<std::string>, std::vector<std::string>> preprocessTokens(DB::Lexer& lexer) const;

    std::vector<std::string> predictNextWords(DB::Lexer& lexer);

    void addQuery(DB::Lexer& lexer);
};
