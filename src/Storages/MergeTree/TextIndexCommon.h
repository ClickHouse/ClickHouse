#pragma once
#include <Core/Names.h>
#include <roaring/roaring.hh>
#include <Core/Types.h>
#include <absl/container/flat_hash_map.h>
#include <mutex>
#include <string_view>
#include <unordered_set>

namespace DB
{

enum class TextSearchMode : uint8_t
{
    Any,
    All,
};

using PostingList = roaring::Roaring;
using PostingListPtr = std::shared_ptr<PostingList>;

/// Closed range of rows.
struct RowsRange
{
    size_t begin;
    size_t end;

    RowsRange() = default;
    RowsRange(size_t begin_, size_t end_) : begin(begin_), end(end_) {}

    bool intersects(const RowsRange & other) const;
    std::optional<RowsRange> intersectWith(const RowsRange & other) const;
};

/// Stores information about posting list for a token.
struct TokenPostingsInfo
{
    UInt64 header = 0;
    UInt32 cardinality = 0;
    std::vector<UInt64> offsets;
    std::vector<RowsRange> ranges;
    PostingListPtr embedded_postings;

    /// Returns indexes of posting list blocks to read for the given range of rows.
    std::vector<size_t> getBlocksToRead(const RowsRange & range) const;
    size_t bytesAllocated() const;
};

using TokenPostingsInfoPtr = std::shared_ptr<TokenPostingsInfo>;
using TokenToPostingsInfosMap = absl::flat_hash_map<String, TokenPostingsInfoPtr>;

class TokensCardinalitiesCache
{
public:
    using CardinalityMap = std::unordered_map<std::string_view, size_t>;
    TokensCardinalitiesCache(std::vector<String> all_search_tokens_, TextSearchMode global_search_mode_);

    void update(const String & part_name, const TokenToPostingsInfosMap & token_infos, size_t num_tokens, size_t total_rows);
    void sortTokens(std::vector<String> & tokens) const;
    std::optional<size_t> getNumTokens(const String & part_name) const;

private:
    const std::vector<String> all_search_tokens;
    const TextSearchMode global_search_mode;

    struct CardinalityAggregate
    {
        size_t cardinality = 0;
        size_t checked_rows = 0;

        bool operator==(const CardinalityAggregate & other) const = default;
    };

    using CardinalitiesMap = std::unordered_map<String, CardinalityAggregate>;
    mutable std::mutex mutex;
    CardinalitiesMap cardinalities TSA_GUARDED_BY(mutex);
    std::unordered_map<String, size_t> num_tokens_by_part TSA_GUARDED_BY(mutex);
};

}
