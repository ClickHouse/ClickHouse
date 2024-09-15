#include <cstddef>
#include <set>
#include <unordered_map>
#include <vector>
#include <string>
#include <span>
#include <functional>


// Hashes/equals for heterogeneous lookups
struct TransparentStringHash {
    using is_transparent = void;
    [[nodiscard]] size_t operator()(const char *txt) const;
    [[nodiscard]] size_t operator()(std::string_view txt) const;
    [[nodiscard]] size_t operator()(const std::string &txt) const;
};


struct TransparentVectorHash {
    using is_transparent = void;
    [[nodiscard]] size_t operator()(const std::vector<std::string>& v) const;
    [[nodiscard]] size_t operator()(std::span<const std::string> span) const;
};


struct TransparentVectorEqual {
    using is_transparent = void;
    [[nodiscard]] bool operator()(const std::vector<std::string>& lhs, const std::vector<std::string>& rhs) const;
    [[nodiscard]] bool operator()(std::span<const std::string> lhs, const std::vector<std::string>& rhs) const;
    [[nodiscard]] bool operator()(const std::vector<std::string>& lhs, std::span<const std::string> rhs) const;
};




// NGramMap Class
class NGram {
public:
    struct Count {
        size_t cnt = 0;
        size_t last_timestamp = 1;
    };

    struct WeightedStats {
        long double weighted_types_count = 0.0l;
        long double weighted_count = 0.0l;
    };

    struct UnigramsWithStats {
        using UnigramMapType = std::unordered_map<std::string, Count, TransparentStringHash, std::equal_to<>>;

        UnigramMapType unigram_map;
        WeightedStats stats;

        long double getWeightedTypesCount() const;
        long double getWeightedCount() const;
        long double getWeightedWordCount(const std::string& word) const;
        long double getWeightedWordTypesCount(const std::string& word) const;
    };

    struct SuffixStats {
        std::unordered_map<std::string, long double> ngrams_with_word_weighted_count;
        long double total_weighted_types_count = 0.0l;
    };

    using MapType = std::unordered_map<std::vector<std::string>, UnigramsWithStats, TransparentVectorHash, TransparentVectorEqual>;
    using MapTypeForSuffixLookup = std::unordered_map<std::vector<std::string>, std::vector<const UnigramsWithStats*>, TransparentVectorHash, TransparentVectorEqual>;

    using MapTypeForSuffixLookup2 = std::unordered_map<std::vector<std::string>, SuffixStats, TransparentVectorHash, TransparentVectorEqual>;

    explicit NGram(size_t context_size);

    template <typename Key>
    UnigramsWithStats& operator[](Key&& key) {
        validate_size(key);
        auto& result = map[std::forward<Key>(key)];
        return result;
    }

    UnigramsWithStats& operator[](std::span<const std::string> span_key) {
        validate_size(span_key);
        if (this->contains(span_key)) {
            return *(this->find(span_key));
        }
        return (*this)[std::vector<std::string>(span_key.begin(), span_key.end())];
    }

    const SuffixStats* getSuffixStats(std::span<const std::string> span_key) const;
    const SuffixStats* getSuffixStats(const std::vector<std::string>& key) const;
    bool contains(std::span<const std::string> span_key) const;
    bool contains(const std::vector<std::string>& key) const;
    size_t size() const;
    const auto& at(const std::vector<std::string>& key) const;
    const UnigramsWithStats& at(std::span<const std::string> span_key) const;

    auto begin() { return map.begin(); }
    auto end() { return map.end(); }
    auto begin() const { return map.begin(); }
    auto end() const { return map.end(); }
    void printMap() const;
    void update(std::span<const std::string> context, const std::string& word, size_t timestamp);

private:
    void validate_size(const std::vector<std::string>& key) const;
    void validate_size(std::span<const std::string>& key) const;
    void updateLastWords(NGram::UnigramsWithStats& last_words, const std::string& word, size_t timestamp);

    UnigramsWithStats* find(std::span<const std::string> span_key);
    UnigramsWithStats* find(const std::vector<std::string>& key);

    MapType map;
    MapTypeForSuffixLookup2 suffix_map;
    const size_t context_size;
    SuffixStats bigram_suffix_stats;
};
// ------------------------------------------------------


class KneserNey {
private:
    struct WordWithCount {
        std::string word;
        size_t cnt;
        size_t last_timestamp;

        long double getWeightedScore() const;
        bool operator==(const WordWithCount& other) const;
        bool operator>(const WordWithCount& other) const;
        bool operator<(const WordWithCount& other) const;
    };


    const size_t order;
    const long double discount = 0.1;
    size_t timestamp = 1;
    std::vector<NGram> ngrams; // ngrams from the lowest context size (1) to highest (order - 1)
    std::set<WordWithCount> words_sorted_by_freq;

    std::vector<NGram> initializeNgrams(size_t order_) const;
    size_t n_most_frequent_to_rank = 1000;

    bool isContextNew(std::span<const std::string> context) const;
    std::pair<long double, long double> continuationCounts(const std::string& word, std::span<const std::string> context) const;
    long double prefixTypesCounts(std::span<const std::string> context) const;
    long double prefixCounts(std::span<const std::string> context) const;
    std::pair<long double, long double> calcAlphaGamma(const std::string& word, std::span<const std::string> context) const;
    void updateUnigrams(NGram::UnigramsWithStats& unigrams, const std::string& word, size_t timestamp);
    long double unigramScore(const std::string& word) const;

public:
    NGram::UnigramsWithStats unigrams;

    explicit KneserNey(size_t order);
    ~KneserNey() = default;

    bool empty() const {return unigrams.unigram_map.empty();}
    void printCounts() const;
    long double score(std::string word, std::span<const std::string> context) const;
    size_t totalNgramsCount() const;
    std::vector<std::string> predictNext(std::span<const std::string> context, size_t n_words_to_predict) const;
    std::string predictNext(std::span<const std::string> context) const;
    std::pair<std::string, long double> predictNextWithProb(std::span<const std::string> context) const;

    void addExample(std::span<const std::string> tokens);
    void addFullQuery(std::span<const std::string> tokens);
    void incTimestamp() {timestamp ++;}
};
