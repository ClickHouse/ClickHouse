#include "config.h"

#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNSearchUtils.h>
#include <Storages/MergeTree/RangesInDataPart.h>

using namespace DB;

/// Validate ANNSearchResults contract: block_coords and distances must have equal sizes.
TEST(ANNExecutionTest, ANNSearchResultsSizeContract)
{
    ANNSearchResults results;
    EXPECT_TRUE(results.block_coords.empty());
    EXPECT_TRUE(results.distances.empty());
    EXPECT_EQ(results.block_coords.size(), results.distances.size());

    results.block_coords.push_back({1, 42});
    results.distances.push_back(0.5f);
    results.block_coords.push_back({2, 100});
    results.distances.push_back(1.2f);
    EXPECT_EQ(results.block_coords.size(), results.distances.size());
}

/// Validate ANNSearchParameters fields can be constructed and moved.
TEST(ANNExecutionTest, ANNSearchParametersConstruction)
{
    ANNSearchParameters params;
    params.column = "emb";
    params.distance_function = "L2Distance";
    params.limit = 10;
    params.reference_vector = {1.0, 2.0, 3.0, 4.0};
    params.rescoring_factor = 2;
    params.additional_filters_present = true;

    EXPECT_EQ(params.column, "emb");
    EXPECT_EQ(params.distance_function, "L2Distance");
    EXPECT_EQ(params.limit, 10u);
    EXPECT_EQ(params.reference_vector.size(), 4u);
    EXPECT_EQ(params.rescoring_factor, 2u);
    EXPECT_TRUE(params.additional_filters_present);

    /// Move semantics
    ANNSearchParameters moved = std::move(params);
    EXPECT_EQ(moved.column, "emb");
    EXPECT_EQ(moved.reference_vector.size(), 4u);
}

/// K-2 tri-state: nullopt => unindexed part
TEST(ANNExecutionTest, TriState_Unindexed)
{
    RangesInDataPartReadHints hints;
    EXPECT_FALSE(hints.ann_search_results.has_value());
}

/// K-2 tri-state: has_value() && empty block_coords => covered but no hits
TEST(ANNExecutionTest, TriState_CoveredNoHits)
{
    RangesInDataPartReadHints hints;
    hints.ann_search_results = ANNSearchResults{};

    EXPECT_TRUE(hints.ann_search_results.has_value());
    EXPECT_TRUE(hints.ann_search_results->block_coords.empty());
    EXPECT_TRUE(hints.ann_search_results->distances.empty());
}

/// K-2 tri-state: has_value() && non-empty block_coords => indexed with hits
TEST(ANNExecutionTest, TriState_IndexedWithHits)
{
    RangesInDataPartReadHints hints;
    ANNSearchResults results;
    results.block_coords = {{10, 0}, {10, 5}, {11, 3}};
    results.distances = {0.1f, 0.2f, 0.3f};
    hints.ann_search_results = std::move(results);

    EXPECT_TRUE(hints.ann_search_results.has_value());
    EXPECT_FALSE(hints.ann_search_results->block_coords.empty());
    EXPECT_EQ(hints.ann_search_results->block_coords.size(), 3u);
    EXPECT_EQ(hints.ann_search_results->distances.size(), 3u);
}

/// Duplicate (block_number, block_offset) pairs: last one wins in unordered_map
TEST(ANNExecutionTest, DuplicateBlockCoords)
{
    ANNSearchResults results;
    results.block_coords = {{5, 10}, {5, 10}};
    results.distances = {1.0f, 2.0f};

    /// Build lookup table the same way as fillDistanceColumnAndFilterForANNSearch
    struct PairHash
    {
        size_t operator()(const std::pair<UInt64, UInt64> & p) const
        {
            return std::hash<UInt64>()(p.first) ^ (std::hash<UInt64>()(p.second) * 0x9e3779b97f4a7c15ULL);
        }
    };
    std::unordered_map<std::pair<UInt64, UInt64>, Float32, PairHash> lookup;
    for (size_t i = 0; i < results.block_coords.size(); ++i)
        lookup[results.block_coords[i]] = results.distances[i];

    /// For duplicate keys, last insertion wins
    EXPECT_EQ(lookup.size(), 1u);
    auto key = std::make_pair(UInt64(5), UInt64(10));
    EXPECT_FLOAT_EQ(lookup[key], 2.0f);
}

/// ReadHints coexistence: both vector_search_results and ann_search_results can be set
/// (though in practice only one should be active per query)
TEST(ANNExecutionTest, ReadHintsCoexistence)
{
    RangesInDataPartReadHints hints;
    hints.vector_search_results = NearestNeighbours{{1, 2, 3}, {{0.1f, 0.2f, 0.3f}}};
    hints.ann_search_results = ANNSearchResults{{{10, 0}}, {0.5f}};

    EXPECT_TRUE(hints.vector_search_results.has_value());
    EXPECT_TRUE(hints.ann_search_results.has_value());
}

/// ANNSearchParameters default values
TEST(ANNExecutionTest, ParameterDefaults)
{
    ANNSearchParameters params;
    params.column = "emb";
    params.distance_function = "L2Distance";
    params.limit = 5;
    params.reference_vector = {0.0, 0.0};

    EXPECT_EQ(params.rescoring_factor, 1u);
    EXPECT_FALSE(params.additional_filters_present);
}
