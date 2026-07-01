#include "config.h"

#if USE_USEARCH

#include <Storages/MergeTree/ScoredSearch/VectorScorer.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndexVectorSimilarity.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>

namespace ProfileEvents
{
    extern const Event USearchSearchCount;
    extern const Event USearchSearchVisitedMembers;
    extern const Event USearchSearchComputedDistances;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace Setting
{
    extern const SettingsUInt64 hnsw_candidate_list_size_for_search;
}

VectorScorer::VectorScorer(
    MergeTreeIndexPtr vector_index_,
    VectorWithMemoryTracking<Float64> reference_vector_,
    size_t top_k_)
    : RowScorer(top_k_)
    , vector_index(std::move(vector_index_))
    , reference_vector(std::move(reference_vector_))
{
}

ScoreDirection VectorScorer::getSortDirection() const
{
    /// All USearch metrics publish "lower is better":
    /// - `L2Distance` (`l2sq`) returns the squared Euclidean distance.
    /// - `cosineDistance` (`cos`) returns `1 - cos(a, b)`, in `[0, 2]`.
    /// - `dotProduct` (`ip`) returns `1 - dot(a, b)`.
    return ScoreDirection::Ascending;
}

void VectorScorer::validate(const ContextPtr & context) const
{
    const auto * index_metadata = typeid_cast<const MergeTreeIndexVectorSimilarity *>(vector_index.get());
    if (!index_metadata)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "VectorScorer expects a vector similarity index, got '{}'", vector_index->index.type);

    if (reference_vector.size() != index_metadata->getDimensions())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "The dimension of the reference vector in the query ({}) does not match the dimension in the index ({})",
            reference_vector.size(), index_metadata->getDimensions());
    }

    checkVectorIsSane(
        reference_vector.data(), reference_vector.size(),
        index_metadata->getScalarKind(), ErrorCodes::INCORRECT_QUERY,
        "reference vector in the SELECT query");

    if (context->getSettingsRef()[Setting::hnsw_candidate_list_size_for_search] == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'hnsw_candidate_list_size_for_search' must not be 0");
}

RowScorer::ScoreResult VectorScorer::scorePart(const DataPartPtr & part, const Prefilter & prefilter, ContextPtr context)
{
    /// Current vector search is not supported for partially materialzied indexes,
    /// because fallback to exact search is not implemented yet.
    if (!vector_index->getDeserializedFormat(part->checksums, vector_index->getFileName(), &part->getDataPartStorage()))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Part {} of table {} does not have files of the vector similarity index '{}'. "
            "Run ALTER TABLE ... MATERIALIZE INDEX to build the index for existing parts",
            part->name, part->storage.getStorageID().getNameForLogs(), vector_index->index.name);
    }

    auto reader_settings = MergeTreeReaderSettings::createFromContext(context);
    size_t skip_index_granularity = vector_index->index.granularity;
    size_t marks_count = part->index_granularity->getMarksCountForSkipIndex(skip_index_granularity);

    if (marks_count == 0)
        return {};

    if (marks_count > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Vector similarity indexes with more than one granule per part are not supported by table functions");

    MarkRanges index_ranges = {MarkRange(0, 1)};

    MergeTreeIndexReader reader(
        vector_index,
        part,
        marks_count,
        index_ranges,
        context->getMarkCache().get(),
        context->getUncompressedCache().get(),
        context->getVectorSimilarityIndexCache().get(),
        reader_settings);

    MergeTreeIndexGranulePtr granule;
    reader.read(0, /*condition=*/ nullptr, granule);

    if (!granule || granule->empty())
        return {};

    const auto & vector_granule = typeid_cast<const MergeTreeIndexGranuleVectorSimilarity &>(*granule);
    chassert(vector_granule.index);

    if (reference_vector.size() != vector_granule.index->dimensions())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "The dimension of the reference vector in the query ({}) does not match the dimension in the index ({})",
            reference_vector.size(), vector_granule.index->dimensions());
    }

    checkVectorIsSane(
        reference_vector.data(), reference_vector.size(),
        vector_granule.scalar_kind, ErrorCodes::INCORRECT_QUERY,
        "reference vector in the SELECT query");

    size_t expansion_search = context->getSettingsRef()[Setting::hnsw_candidate_list_size_for_search];
    if (expansion_search == 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting 'hnsw_candidate_list_size_for_search' must not be 0");

    auto search_result = prefilter
        ? vector_granule.index->filtered_search(
            reference_vector.data(), getTopK(),
            [&prefilter](UInt32 row_id) { return prefilter->contains(row_id); },
            USearchIndex::any_thread(), /*exact=*/ false, expansion_search)
        : vector_granule.index->search(
            reference_vector.data(), getTopK(),
            USearchIndex::any_thread(), /*exact=*/ false, expansion_search);

    if (!search_result)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not search in vector similarity index. Error: {}", search_result.error.release());

    ProfileEvents::increment(ProfileEvents::USearchSearchCount);
    ProfileEvents::increment(ProfileEvents::USearchSearchVisitedMembers, search_result.visited_members);
    ProfileEvents::increment(ProfileEvents::USearchSearchComputedDistances, search_result.computed_distances);

    std::vector<std::pair<UInt64, Float32>> result;
    result.reserve(search_result.size());

    for (size_t i = 0; i < search_result.size(); ++i)
        result.emplace_back(search_result[i].member.key, static_cast<Float32>(search_result[i].distance));

    std::ranges::sort(result, [](const auto & lhs, const auto & rhs)
    {
        return std::tie(lhs.second, lhs.first) < std::tie(rhs.second, rhs.first);
    });

    return result;
}

}

#endif
