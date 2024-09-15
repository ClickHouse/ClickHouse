#pragma once

#include <cstddef>
#include <unordered_map>
#include <string>
#include <span>
#include <utility>
#include <vector>

struct ThreeStrings {
    std::string first;
    std::string second;
    std::string third;
    bool operator==(const ThreeStrings& other) const {
        return first == other.first && second == other.second && third == other.third;
    }
};

struct TwoStrings {
    std::string first;
    std::string second;
    bool operator==(const TwoStrings& other) const {
        return first == other.first && second == other.second;
    }
};

namespace std {
    template <>
    struct hash<ThreeStrings> {
        std::size_t operator()(const ThreeStrings& key) const {
            return hash<std::string>()(key.first) ^
                   (hash<std::string>()(key.second) << 1) ^
                   (hash<std::string>()(key.third) << 2);
        }
    };
    template <>
    struct hash<TwoStrings> {
        std::size_t operator()(const TwoStrings& key) const {
            return hash<std::string>()(key.first) ^
                   (hash<std::string>()(key.second) << 1);
        }
    };
};

class KneserNey {
private:

    struct Count {
        size_t cnt = 0;
        size_t last_timestamp = 1;
    };

    size_t order = 4;
    double discount = 0.1;
    size_t timestamp = 1;
    std::unordered_map<ThreeStrings, std::unordered_map<std::string, Count>> grams4;
    std::unordered_map<TwoStrings, std::unordered_map<std::string, Count>> grams3;
    std::unordered_map<std::string, std::unordered_map<std::string, Count>> grams2;
    std::unordered_map<std::string, Count> unigrams;

    bool isContextNew(std::span<const std::string> context) const;

    double getWeightedTypesCount(const std::unordered_map<std::string, Count>& last_words_map) const;

    double getWeightedCount(const std::unordered_map<std::string, Count>& last_words_map) const;

    double getWeightedWordCount(const std::unordered_map<std::string, Count>& last_words_map, const std::string& word) const;

    double getWeightedWordTypesCount(const std::unordered_map<std::string, Count>& last_words_map, const std::string& word) const;

    double score(std::string word, std::span<const std::string> context) const;

    std::pair<double, double> continuationCounts(std::string word, std::span<const std::string> context) const;

    double prefixCounts(std::span<const std::string> context) const;

    std::pair<double, double> calcAlphaGamma(const std::string& word, std::span<const std::string> context) const;

    double unigramScore(const std::string& word) const;

public:
    bool empty() const {return unigrams.empty();}

    void addExample(std::span<const std::string> tokens);

    void addFullQuery(std::span<const std::string> tokens);

    void incTimestamp() {timestamp ++;}

    //std::string predictNext(std::span<const std::string> context, std::span<const std::string> possible_words);

    //std::vector<std::string> predictNext(std::span<const std::string> context, std::span<const std::string> possible_words, size_t n);

    std::string predictNext(std::span<const std::string> context) const;

    std::pair<std::string, double> predictNextWithProb(std::span<const std::string> context) const;

    std::vector<std::string> predictNext(std::span<const std::string> context, size_t predictions_num) const;

    //std::vector<std::string> predictNext(std::span<const std::string> context, size_t n);

};
