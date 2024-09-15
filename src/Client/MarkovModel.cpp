#include "MarkovModel.h"

#include <cassert>
#include <cmath>
#include <queue>
#include <vector>


bool KneserNey::isContextNew(std::span<const std::string> context) const {
    if (context.size() == 3) {
        auto three_strings_context = ThreeStrings{context[0], context[1], context[2]};
        return grams4.find(three_strings_context) == grams4.end();
    } else if (context.size() == 2) {
        auto two_strings_context = TwoStrings{context[0], context[1]};
        return grams3.find(two_strings_context) == grams3.end();
    } else if (context.size() == 1) {
        return grams2.find(context[0]) == grams2.end();
    } else {
        throw std::invalid_argument("Context must have 1, 2, or 3 elements");
    }
}

double KneserNey::getWeightedTypesCount(const std::unordered_map<std::string, Count>& last_words_map) const {
    double total = 0;
    for (const auto& [last_word, count]: last_words_map) {
        total += 1 * std::sqrt(static_cast<double>(count.last_timestamp));
    }
    return total;
}

double KneserNey::getWeightedCount(const std::unordered_map<std::string, Count>& last_words_map) const {
    double total = 0;
    for (const auto& [last_word, count]: last_words_map) {
        total += count.cnt * std::sqrt(static_cast<double>(count.last_timestamp));
    }
    return total;
}

double KneserNey::getWeightedWordCount(const std::unordered_map<std::string, Count>& last_words_map, const std::string& word) const {
    if (last_words_map.contains(word)) {
        return last_words_map.at(word).cnt * std::sqrt(static_cast<double>(last_words_map.at(word).last_timestamp));
    }
    return 0.0;
}

double KneserNey::getWeightedWordTypesCount(const std::unordered_map<std::string, Count>& last_words_map, const std::string& word) const {
    if (last_words_map.contains(word)) {
        return 1 * std::sqrt(static_cast<double>(last_words_map.at(word).last_timestamp));
    }
    return 0.0;
}

double KneserNey::score(std::string word, std::span<const std::string> context) const {
    if (context.empty()) {
        auto res = unigramScore(word);
        return res;
    }
    double alpha = -1;
    double gamma = -1;
    if (isContextNew(context)) {
        alpha = 0;
        gamma = 1;
    } else {
        auto alpha_and_gamma = calcAlphaGamma(word, context);
        alpha = alpha_and_gamma.first;
        gamma = alpha_and_gamma.second;
    }
    auto context_without_first = std::span<const std::string>(context.begin() + 1, context.end());
    return alpha + gamma * score(word, context_without_first);
}

std::pair<double, double> KneserNey::continuationCounts(std::string word, std::span<const std::string> context) const {
    double higher_order_ngrams_with_word_count = 0;
    double total = 0;
    if (context.size() == 3) {
        throw std::invalid_argument("Context must have 1 or 2 elements");
    }
    if (context.size() == 2) {
        for (const auto& [three_strings_context, last_words]: grams4) {
            if (three_strings_context.second == context[0] && three_strings_context.third == context[1]) {
                total += getWeightedTypesCount(last_words);
                higher_order_ngrams_with_word_count += getWeightedWordTypesCount(last_words, word);
            }
        }
    }
    if (context.size() == 1) {
        for (const auto& [two_string_context, last_words]: grams3) {
            if (two_string_context.second == context[0]) {
                total += getWeightedTypesCount(last_words);
                higher_order_ngrams_with_word_count += getWeightedWordTypesCount(last_words, word);
            }
        }
    }
    if (context.empty()) {
        for (const auto& [one_string_context, last_words]: grams2) {
            total += getWeightedTypesCount(last_words);
            higher_order_ngrams_with_word_count += getWeightedWordTypesCount(last_words, word);
        }
    }
    return {higher_order_ngrams_with_word_count, total};
}

double KneserNey::prefixCounts(std::span<const std::string> context) const {
    double result = 0;
    if (context.size() == 3) {
        auto three_strings_context = ThreeStrings{context[0], context[1], context[2]};
        if (grams4.contains(three_strings_context)) {
            result = getWeightedTypesCount(grams4.at(three_strings_context));
        }
    } if (context.size() == 2) {
        auto two_strings_context = TwoStrings{context[0], context[1]};
        if (grams3.contains(two_strings_context)) {
            result = getWeightedTypesCount(grams3.at(two_strings_context));
        }
    } if (context.size() == 1) {
        if (grams2.contains(context[0])) {
            result = getWeightedTypesCount(grams2.at(context[0]));
        }
    }
    if (context.empty()) {
        throw std::invalid_argument("Context must have 1, 2, or 3 elements");
    }
    return result;
}

std::pair<double, double> KneserNey::calcAlphaGamma(const std::string& word, std::span<const std::string> context) const {
    double word_continuation_count = 0;
    double total_count = 0;
    if (context.size() + 1 == order) {
        auto three_strings_context = ThreeStrings{context[0], context[1], context[2]};
        word_continuation_count = getWeightedWordCount(grams4.at(three_strings_context), word);
        total_count = getWeightedCount(grams4.at(three_strings_context));
    } else {
        auto pair = continuationCounts(word, context);
        word_continuation_count = pair.first;
        total_count = pair.second;
    }
    double alpha = std::max(word_continuation_count - discount, 0.0) / total_count;
    double gamma = discount * static_cast<double>(prefixCounts(context)) / total_count;
    return {alpha, gamma};
}

double KneserNey::unigramScore(const std::string& word) const {
    auto [word_continuation_count, total_count] = continuationCounts(word, {});
    return static_cast<double>(word_continuation_count) / static_cast<double>(total_count);
}


void KneserNey::addExample(std::span<const std::string> tokens) {
    // std::cout << "ADDING EXAMPLE: \n";
    // for (const auto& token: tokens) {
    //     std::cout << token << "\n";
    // }
    assert(tokens.size() == order);

    grams4[ThreeStrings{tokens[0], tokens[1], tokens[2]}][tokens[3]].cnt += 1;
    grams4[ThreeStrings{tokens[0], tokens[1], tokens[2]}][tokens[3]].last_timestamp = timestamp;

    grams3[TwoStrings{tokens[1], tokens[2]}][tokens[3]].cnt += 1;
    grams3[TwoStrings{tokens[1], tokens[2]}][tokens[3]].last_timestamp = timestamp;

    grams2[tokens[2]][tokens[3]].cnt += 1;
    grams2[tokens[2]][tokens[3]].last_timestamp = timestamp;

    unigrams[tokens[3]].cnt += 1;
    unigrams[tokens[3]].last_timestamp += timestamp;
}

void KneserNey::addFullQuery(std::span<const std::string> tokens) {
    // std::cout << "ADDING EXAMPLE: \n";
    // for (const auto& token: tokens) {
    //     std::cout << token << "\n";
    // }
    if (tokens.size() < order) {
        throw std::invalid_argument("LOL");
    }
    for (const auto & token : tokens) {
        unigrams[token].cnt += 1;
        unigrams[token].last_timestamp = timestamp;
    }
    for (size_t i = 0; i + 1 != tokens.size(); ++i) {
        grams2[tokens[i]][tokens[i + 1]].cnt += 1;
        grams2[tokens[i]][tokens[i + 1]].last_timestamp = timestamp;
    }
    for (size_t i = 0; i + 2 != tokens.size(); ++i) {
        grams3[{tokens[i], tokens[i + 1]}][tokens[i + 2]].cnt += 1;
        grams3[{tokens[i], tokens[i + 1]}][tokens[i + 2]].last_timestamp = timestamp;
    }
    for (size_t i = 0; i + 3 != tokens.size(); ++i) {
        grams4[{tokens[i], tokens[i + 1], tokens[i + 2]}][tokens[i + 3]].cnt += 1;
        grams4[{tokens[i], tokens[i + 1], tokens[i + 2]}][tokens[i + 3]].last_timestamp = timestamp;
    }
}

std::string KneserNey::predictNext(std::span<const std::string> context) const {
    if (context.size() > order - 1) {
        context = context.subspan(context.size() - (order - 1), order - 1);
    }

    std::string most_probable_word;
    double prob = -1;
    for (const auto& [word, count]: unigrams) {
        auto word_score = score(word, context);

        if (word_score > prob) {
            prob = word_score;
            most_probable_word = word;
        }
    }
    
    return most_probable_word;
}

std::vector<std::string> KneserNey::predictNext(std::span<const std::string> context, size_t predictions_num) const {
    if (context.size() > order - 1) {
        context = context.subspan(context.size() - (order - 1), order - 1);
    }

    using WordProbPair = std::pair<double, std::string>;
    auto compare = [](const WordProbPair& a, const WordProbPair& b) {
        return a.first > b.first;
    };
    
    // Priority queue to keep top predictions
    std::priority_queue<WordProbPair, std::vector<WordProbPair>, decltype(compare)> top_words(compare);

    // Iterate through unigrams and calculate scores
    for (const auto& [word, count]: unigrams) {
        double word_score = score(word, context);
        top_words.push({word_score, word});

        // If we exceed the number of predictions required, remove the lowest probability word
        if (top_words.size() > predictions_num) {
            top_words.pop();
        }
    }

    // Prepare the result vector
    std::vector<std::string> most_probable_words;
    while (!top_words.empty()) {
        most_probable_words.push_back(top_words.top().second);
        top_words.pop();
    }

    // Reverse to maintain order from highest to lowest probability
    std::reverse(most_probable_words.begin(), most_probable_words.end());

    // Return the number of available predictions (<= predictions_num)
    return most_probable_words;
}


std::pair<std::string, double> KneserNey::predictNextWithProb(std::span<const std::string> context) const {
    if (context.size() > order - 1) {
        context = context.subspan(context.size() - (order - 1), order - 1);
    }

    std::string most_probable_word;
    double prob = -1;
    for (const auto& [word, count]: unigrams) {
        auto word_score = score(word, context);

        if (word_score > prob) {
            prob = word_score;
            most_probable_word = word;
        }
    }
    
    return {most_probable_word, prob};
}
