#include "MarkovModelFast.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <iostream>
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>
#include <string>
#include <span>
#include <functional>

// ------------------------------------------------------

[[nodiscard]] size_t TransparentStringHash::operator()(const char *txt) const {
    return std::hash<std::string_view>{}(txt);
}

[[nodiscard]] size_t TransparentStringHash::operator()(std::string_view txt) const {
    return std::hash<std::string_view>{}(txt);
}

[[nodiscard]] size_t TransparentStringHash::operator()(const std::string &txt) const {
    return std::hash<std::string>{}(txt);
}



[[nodiscard]] size_t TransparentVectorHash::operator()(const std::vector<std::string>& v) const {
    std::size_t seed = 0;
    for (const auto& str : v) {
        seed ^= std::hash<std::string>{}(str) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
}

[[nodiscard]] size_t TransparentVectorHash::operator()(std::span<const std::string> span) const {
    std::size_t seed = 0;
    for (const auto& str : span) {
        seed ^= std::hash<std::string>{}(str) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
}


[[nodiscard]] bool TransparentVectorEqual::operator()(const std::vector<std::string>& lhs, const std::vector<std::string>& rhs) const {
    return lhs == rhs;
}

[[nodiscard]] bool TransparentVectorEqual::operator()(std::span<const std::string> lhs, const std::vector<std::string>& rhs) const {
    return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

[[nodiscard]] bool TransparentVectorEqual::operator()(const std::vector<std::string>& lhs, std::span<const std::string> rhs) const {
    return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin());
}


// ------------------------------------------------------
// NGramMap Class

// UnigramMapWithStats

long double NGram::UnigramsWithStats::getWeightedTypesCount() const {
    return stats.weighted_types_count;
}

long double NGram::UnigramsWithStats::getWeightedCount() const {
    return stats.weighted_count;
}

long double NGram::UnigramsWithStats::getWeightedWordCount(const std::string& word) const {
    if (unigram_map.contains(word)) {
        return unigram_map.at(word).cnt * std::sqrt(static_cast<long double>(unigram_map.at(word).last_timestamp));
    }
    return 0.0;
}

long double NGram::UnigramsWithStats::getWeightedWordTypesCount(const std::string& word) const {
    if (unigram_map.contains(word)) {
        return 1 * std::sqrt(static_cast<long double>(unigram_map.at(word).last_timestamp));
    }
    return 0.0;
}

//

NGram::NGram(size_t context_size_) : context_size(context_size_) {}

NGram::UnigramsWithStats* NGram::find(std::span<const std::string> span_key) {
    auto it = map.find(span_key);
    if (it != map.end()) {
        return &it->second;
    }
    return nullptr;
}

NGram::UnigramsWithStats* NGram::find(const std::vector<std::string>& key) {
    auto it = map.find(key);
    if (it != map.end()) {
        return &it->second;
    }
    return nullptr;
}

const NGram::SuffixStats* NGram::getSuffixStats(std::span<const std::string> span_key) const {
    if (context_size == 1) {
        return &bigram_suffix_stats;
    }
    auto it = suffix_map.find(span_key);
    if (it != suffix_map.end()) {
        return &it->second;
    }
    return nullptr;
}

const NGram::SuffixStats* NGram::getSuffixStats(const std::vector<std::string>& key) const  {
    if (context_size == 1) {
        return &bigram_suffix_stats;
    }
    auto it = suffix_map.find(key);
    if (it != suffix_map.end()) {
        return &it->second;
    }
    return nullptr;
}

bool  NGram::contains(std::span<const std::string> span_key) const {
    auto it = map.find(span_key);
    bool result = it != map.end();
    return result;
}

bool NGram::contains(const std::vector<std::string>& key) const {
    return map.find(key) != map.end();
}

size_t NGram::size() const {
    return map.size();
}

const auto& NGram::at(const std::vector<std::string>& key) const {
    return map.at(key);
}

const NGram::UnigramsWithStats& NGram::at(std::span<const std::string> span_key) const {
    auto it = map.find(span_key);
    if (it == map.end()) {
        throw std::out_of_range("NGramMap key not found");
    }
    return it->second;
}

void NGram::printMap() const {
    std::cout << "START OF THE MAP WITH CONTEXT SIZE: " << context_size << " ________________\n";
    for (const auto& [context, words_map]: *this) {
        std::cout << "Key: ";
        for (const auto& context_word: context) {
            std::cout << context_word << ", ";
        }
        std::cout << "\n";
        for (const auto& [word, count]: words_map.unigram_map) {
            std::cout << "Value: " << word << ", CNT: " << count.cnt << "\n";
        }
        
        std::cout << "________________\n";
    }
    std::cout << "END OF THE MAP WITH CONTEXT SIZE: " << context_size << " ________________\n";
}

void NGram::update(std::span<const std::string> context, const std::string& word, size_t timestamp) {
    auto& last_words_to_update = (*this)[context];
    if (context_size == 1) {
        bigram_suffix_stats.total_weighted_types_count -= last_words_to_update.getWeightedTypesCount();
        bigram_suffix_stats.ngrams_with_word_weighted_count[word] -= last_words_to_update.getWeightedWordTypesCount(word);
    } else {
        std::vector<std::string> suffix(context.begin() + 1, context.end());
        suffix_map[suffix].total_weighted_types_count -= last_words_to_update.getWeightedTypesCount();
        suffix_map[suffix].ngrams_with_word_weighted_count[word] -= last_words_to_update.getWeightedWordTypesCount(word);
    }
    updateLastWords(last_words_to_update, word, timestamp);

    if (context_size == 1) {
        bigram_suffix_stats.total_weighted_types_count += last_words_to_update.getWeightedTypesCount();
        bigram_suffix_stats.ngrams_with_word_weighted_count[word] += last_words_to_update.getWeightedWordTypesCount(word);
    } else {
        std::vector<std::string> suffix(context.begin() + 1, context.end());
        suffix_map[suffix].total_weighted_types_count += last_words_to_update.getWeightedTypesCount();
        suffix_map[suffix].ngrams_with_word_weighted_count[word] += last_words_to_update.getWeightedWordTypesCount(word);
    }
}

void NGram::validate_size(const std::vector<std::string>& key) const {
    if (key.size() != context_size) {
        throw std::invalid_argument("NGramMap expects exactly " + std::to_string(context_size) + " elements.");
    }
}

void NGram::validate_size(std::span<const std::string>& key) const {
    if (key.size() != context_size) {
        throw std::invalid_argument("NGramMap expects exactly " + std::to_string(context_size) + " elements.");
    }
}

void NGram::updateLastWords(NGram::UnigramsWithStats& last_words, const std::string& word, size_t timestamp) {
    if (last_words.unigram_map.contains(word)) {
        last_words.stats.weighted_count -= last_words.getWeightedWordCount(word);
        last_words.stats.weighted_types_count -= last_words.getWeightedWordTypesCount(word);
    }

    last_words.unigram_map[word].cnt += 1;
    last_words.unigram_map[word].last_timestamp = timestamp;

    last_words.stats.weighted_count += last_words.getWeightedWordCount(word);
    last_words.stats.weighted_types_count += last_words.getWeightedWordTypesCount(word);

}


// ------------------------------------------------------
// KneserNey class

// WordWithCount
long double KneserNey::WordWithCount::getWeightedScore() const {
    return static_cast<long double>(cnt) * std::sqrt(static_cast<long double>(last_timestamp));
}

bool KneserNey::WordWithCount::operator==(const WordWithCount& other) const {
    return word == other.word;
}

bool KneserNey::WordWithCount::operator>(const WordWithCount& other) const {
    long double this_weighted_score = getWeightedScore();
    long double other_weighted_score = other.getWeightedScore();

    if (this_weighted_score != other_weighted_score) {
        return this_weighted_score < other_weighted_score;  // Descending order of score
    }
    return word > other.word;  // Ascending order of word for equal scores
}

bool KneserNey::WordWithCount::operator<(const WordWithCount& other) const {
    return !(*this > other) && !(*this == other);
}

std::vector<NGram> KneserNey::initializeNgrams(size_t order_) const {
    std::vector<NGram> ngrams_vec;
    if (order_ == 0) {
        throw std::invalid_argument("KneserNey order must be at least 1");
    }

    ngrams_vec.reserve(order_ - 1);
    for (size_t i = 1; i != order_; ++i) {
        ngrams_vec.emplace_back(NGram(i));
    }
    return ngrams_vec;
}

KneserNey::KneserNey(size_t order_): order(order_), ngrams(initializeNgrams(order_)) {}

void KneserNey::printCounts() const {
    for (const auto& map: ngrams) {
        map.printMap();
    }
}

bool KneserNey::isContextNew(std::span<const std::string> context) const {
    size_t context_size = context.size();
    if (context_size == 0 || context.size() + 1 > order) {
        throw std::invalid_argument("KneserNay::isContextNew Context must have from 1 to order - 1 elements");
    }

    return !ngrams[context_size - 1].contains(context);
}

std::pair<long double, long double> KneserNey::continuationCounts(const std::string& word, std::span<const std::string> context) const {
    long double higher_order_ngrams_with_word_count = 0;
    long double total = 0;

    size_t context_size = context.size();

    if (context_size + 2 > order) {
        throw std::invalid_argument("KneserNey::continuationCounts Context must have from 1 to order - 2 elements");
    }

    const auto& ngrams_map = ngrams[context_size];
    const auto* suffix_stats = ngrams_map.getSuffixStats(context);
    total += suffix_stats->total_weighted_types_count;
    if (suffix_stats->ngrams_with_word_weighted_count.contains(word)) {
        higher_order_ngrams_with_word_count += suffix_stats->ngrams_with_word_weighted_count.at(word);
    }

    return {higher_order_ngrams_with_word_count, total};
}

long double KneserNey::prefixTypesCounts(std::span<const std::string> context) const {
    long double result = 0;

    if (context.empty() || context.size() + 1 > order) {
        throw std::invalid_argument("KneserNey::prefixCounts Context must have from 1 to order - 1 elements");
    }

    const auto& ngrams_map = ngrams[context.size() - 1];
    if (ngrams_map.contains(context)) {
        result = ngrams_map.at(context).getWeightedTypesCount();
    }
    return result;
}

long double KneserNey::prefixCounts(std::span<const std::string> context) const {
    long double result = 0;

    if (context.empty() || context.size() + 1 > order) {
        throw std::invalid_argument("KneserNey::prefixCounts Context must have from 1 to order - 1 elements");
    }

    const auto& ngrams_map = ngrams[context.size() - 1];
    if (ngrams_map.contains(context)) {
        result = ngrams_map.at(context).getWeightedCount();
    }
    return result;
}

std::pair<long double, long double> KneserNey::calcAlphaGamma(const std::string& word, std::span<const std::string> context) const {
    long double word_continuation_count = 0;
    long double total_count = 0;
    long double gamma = -1;
    if (context.size() + 1 == order) {
        const auto& highest_order_map = ngrams[context.size() - 1];
        word_continuation_count = highest_order_map.at(context).getWeightedWordCount(word);
        total_count = highest_order_map.at(context).getWeightedCount();
    } else {
        auto pair = continuationCounts(word, context);
        word_continuation_count = pair.first;
        total_count = pair.second;
    }
    long double alpha = std::max(word_continuation_count - discount, 0.0l) / total_count;
    gamma = discount * static_cast<long double>(prefixTypesCounts(context)) / total_count;
    return {alpha, gamma};
}

void KneserNey::updateUnigrams(NGram::UnigramsWithStats& unigrams_to_update, const std::string& word, size_t cur_timestamp) {
    if (unigrams_to_update.unigram_map.contains(word)) {
        unigrams_to_update.stats.weighted_count -= unigrams_to_update.getWeightedWordCount(word);
        unigrams_to_update.stats.weighted_types_count -= unigrams_to_update.getWeightedWordTypesCount(word);

        words_sorted_by_freq.erase(WordWithCount{.word=word, .cnt=unigrams_to_update.unigram_map[word].cnt, .last_timestamp=unigrams_to_update.unigram_map[word].last_timestamp});
    }

    unigrams_to_update.unigram_map[word].cnt += 1;
    unigrams_to_update.unigram_map[word].last_timestamp = cur_timestamp;
    words_sorted_by_freq.insert(WordWithCount{.word=word, .cnt=unigrams_to_update.unigram_map[word].cnt, .last_timestamp=unigrams_to_update.unigram_map[word].last_timestamp});
    unigrams_to_update.stats.weighted_count += unigrams_to_update.getWeightedWordCount(word);
    unigrams_to_update.stats.weighted_types_count += unigrams_to_update.getWeightedWordTypesCount(word);
}

long double KneserNey::unigramScore(const std::string& word) const {
    auto [word_continuation_count, total_count] = continuationCounts(word, {});
    return static_cast<long double>(word_continuation_count) / static_cast<long double>(total_count);
}

long double KneserNey::score(std::string word, std::span<const std::string> context) const {
    if (context.size() + 1 > order) {
        context = std::span<const std::string>(context.begin() + (context.size() + 1) - order, context.end());
    }
    if (context.empty()) {
        auto res = unigramScore(word);
        return res;
    }
    long double alpha = -1;
    long double gamma = -1;
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

size_t KneserNey::totalNgramsCount() const {
    size_t res = 0;
    for (const auto& map: ngrams) {
        res += map.size();
    }
    return res;
}

std::vector<std::string> KneserNey::predictNext(std::span<const std::string> context, size_t n_words_to_predict) const {
    using WordScorePair = std::pair<long double, const std::string*>;

    auto compare = [](const WordScorePair& a, const WordScorePair& b) {
        return a.first > b.first;
    };
    
    std::priority_queue<WordScorePair, std::vector<WordScorePair>, decltype(compare)> top_words(compare);

    size_t count = 0;
    for (const auto& word_with_count : words_sorted_by_freq) {
        if (count >= n_most_frequent_to_rank) break;
        count++;

        long double word_score = score(word_with_count.word, context);
        top_words.push({word_score, &word_with_count.word});

        if (top_words.size() > n_words_to_predict) {
            top_words.pop();
        }
    }

    std::vector<std::string> most_probable_words;
    most_probable_words.reserve(n_words_to_predict);
    while (!top_words.empty()) {
        most_probable_words.push_back(*top_words.top().second);
        top_words.pop();
    }

    std::reverse(most_probable_words.begin(), most_probable_words.end());

    return most_probable_words;
}

std::string KneserNey::predictNext(std::span<const std::string> context) const {
    if (unigrams.unigram_map.empty()) {
        return "";
    }
    long double max_score = -1;
    
    const std::string* best_word = nullptr;

    size_t count = 0;
    for (const auto& word_with_count : words_sorted_by_freq) {
        if (count >= n_most_frequent_to_rank) break;
        count++;

        long double cur_score = score(word_with_count.word, context);
        if (cur_score > max_score) {
            best_word = &word_with_count.word;
            max_score = cur_score;
        }
    }

    return best_word ? *best_word : std::string();

}

std::pair<std::string, long double> KneserNey::predictNextWithProb(std::span<const std::string> context) const {
    if (unigrams.unigram_map.empty()) {
        return {"", 0.0};
    }
    
    long double total_score = 0;
    long double max_score = -1;
    
    const std::string* best_word = nullptr;

    size_t count = 0;
    for (const auto& word_with_count : words_sorted_by_freq) {
        if (count >= n_most_frequent_to_rank) break;
        count++;

        long double cur_score = score(word_with_count.word, context);
        total_score += cur_score;
        if (cur_score > max_score) {
            best_word = &word_with_count.word;
            max_score = cur_score;
        }
    }

    return {best_word ? *best_word : std::string(), max_score / total_score};
}


void KneserNey::addExample(std::span<const std::string> tokens) {
    assert(tokens.size() >= order);

    const auto& last_word = tokens.back();

    for (size_t i = 0; i + 1 < order; ++i) {
        std::span<const std::string> context = tokens.subspan(tokens.size() - order + i, order - (i + 1));
        ngrams[context.size() - 1].update(context, last_word, timestamp);
    }

    updateUnigrams(unigrams, last_word, timestamp);
}


void KneserNey::addFullQuery(std::span<const std::string> tokens) {
    assert(tokens.size() >= order);

    for (size_t i = 0; i + 1 <= order; i++) {
        for (size_t j = 0; j + i != tokens.size(); ++j) {
            if (i == 0) {
                updateUnigrams(unigrams, tokens[j], timestamp);
            } else {
                auto context = tokens.subspan(j, i);
                const std::string& word = tokens[i + j];
                ngrams[context.size() - 1].update(context, word, timestamp);
            }
        }
    }
}

