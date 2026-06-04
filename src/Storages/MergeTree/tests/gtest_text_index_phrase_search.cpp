#include <gtest/gtest.h>

#include <Storages/MergeTree/TextIndexPhraseSearch.h>
#include <Storages/MergeTree/TextIndexPositionData.h>

#include <algorithm>
#include <vector>

using namespace DB;

namespace
{

/// Build a token position list the same way the index does: one RoaringishEntry per
/// (doc_id, position), sorted by (doc_id, group) and with same-bucket entries merged.
std::vector<RoaringishEntry> positions(std::initializer_list<std::pair<UInt32, UInt32>> doc_positions)
{
    std::vector<RoaringishEntry> entries;
    for (auto [doc_id, position] : doc_positions)
        entries.push_back(RoaringishEntry::make(doc_id, position));

    std::sort(entries.begin(), entries.end());

    if (entries.size() > 1)
    {
        size_t out_idx = 0;
        for (size_t i = 1; i < entries.size(); ++i)
        {
            if (entries[out_idx].sameBucket(entries[i]))
                entries[out_idx].mergeBitmap(entries[i]);
            else
                entries[++out_idx] = entries[i];
        }
        entries.resize(out_idx + 1);
    }

    return entries;
}

}

TEST(TextIndexPhraseSearch, ConsecutiveWithinGroupMatches)
{
    /// "a b" with a@1, b@2 in doc 0 -> match.
    auto matching = TextIndexPhraseSearch::phraseSearch({positions({{0, 1}}), positions({{0, 2}})});
    EXPECT_EQ(matching, (std::vector<UInt32>{0}));
}

TEST(TextIndexPhraseSearch, NonConsecutiveDoesNotMatch)
{
    /// "a b" with a@1, b@3 (a token between them) -> no match.
    auto matching = TextIndexPhraseSearch::phraseSearch({positions({{0, 1}}), positions({{0, 3}})});
    EXPECT_TRUE(matching.empty());
}

TEST(TextIndexPhraseSearch, CrossesBitmapGroupBoundary)
{
    /// "a b" with a at the last bit of group 0 (position 63) and b at the first bit of
    /// group 1 (position 64). The shift overflows the 64-bit bitmap, so the match is only
    /// found by the boundary-crossing phase of `intersect`.
    auto matching = TextIndexPhraseSearch::phraseSearch({positions({{0, 63}}), positions({{0, 64}})});
    EXPECT_EQ(matching, (std::vector<UInt32>{0}));

    /// Same boundary, but b is one position too far (position 65) -> no match.
    auto no_match = TextIndexPhraseSearch::phraseSearch({positions({{0, 63}}), positions({{0, 65}})});
    EXPECT_TRUE(no_match.empty());
}

TEST(TextIndexPhraseSearch, ThreeTermPhraseWithDuplicateIntermediateKeys)
{
    /// Regression for the same-bucket merge in `intersect`. Single document:
    ///   term0 positions {63, 65}, term1 positions {64, 66}, term2 positions {67}.
    /// The phrase matches via 65, 66, 67. After the first intersect, the intermediate
    /// list contains two entries with the same (doc=0, group=1) key (one from the
    /// boundary overflow at position 64, one from the within-group match at position 66).
    /// Without merging those before the next step, the second intersect advances past the
    /// first duplicate and misses the match -> false negative.
    auto matching = TextIndexPhraseSearch::phraseSearch(
        {positions({{0, 63}, {0, 65}}), positions({{0, 64}, {0, 66}}), positions({{0, 67}})});
    EXPECT_EQ(matching, (std::vector<UInt32>{0}));
}

TEST(TextIndexPhraseSearch, RepeatedTokenPhrase)
{
    /// Phrase "a a" against input where "a" occurs at positions 1 and 2 -> match.
    /// Both terms share the same position list (the same token).
    auto token_a = positions({{0, 1}, {0, 2}});
    auto matching = TextIndexPhraseSearch::phraseSearch({token_a, token_a});
    EXPECT_EQ(matching, (std::vector<UInt32>{0}));

    /// "a a" against input where "a" occurs only at non-adjacent positions 1 and 3 -> no match.
    auto token_a_gap = positions({{0, 1}, {0, 3}});
    auto no_match = TextIndexPhraseSearch::phraseSearch({token_a_gap, token_a_gap});
    EXPECT_TRUE(no_match.empty());
}

TEST(TextIndexPhraseSearch, MultipleDocumentsAreSeparated)
{
    /// doc 0: "a b" consecutive (match); doc 1: "a" and "b" non-consecutive (no match);
    /// doc 2: "a b" consecutive (match). Only docs 0 and 2 should be returned.
    auto term_a = positions({{0, 5}, {1, 5}, {2, 10}});
    auto term_b = positions({{0, 6}, {1, 8}, {2, 11}});
    auto matching = TextIndexPhraseSearch::phraseSearch({term_a, term_b});
    EXPECT_EQ(matching, (std::vector<UInt32>{0, 2}));
}

TEST(TextIndexPhraseSearch, MissingTermYieldsNoMatch)
{
    /// An empty position list for any term (token absent from the index) -> no match.
    auto matching = TextIndexPhraseSearch::phraseSearch({positions({{0, 1}}), {}});
    EXPECT_TRUE(matching.empty());
}
