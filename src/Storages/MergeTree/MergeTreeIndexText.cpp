#include <Storages/MergeTree/MergeTreeIndexText.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleText::MergeTreeIndexGranuleText()
{
}

void MergeTreeIndexGranuleText::serializeBinary(WriteBuffer & ostr) const
{
}

void MergeTreeIndexGranuleText::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
}

MergeTreeIndexGranuleTextWritable::MergeTreeIndexGranuleTextWritable(
    BloomFilter bloom_filter_,
    std::vector<StringRef> terms_,
    std::vector<roaring::Roaring> posting_lists_,
    std::unique_ptr<Arena> arena_)
    : bloom_filter(bloom_filter_), terms(terms_), posting_lists(posting_lists_), arena(std::move(arena_))
{
}

void MergeTreeIndexGranuleTextWritable::serializeBinary(WriteBuffer & ostr) const
{
}

void MergeTreeIndexGranuleTextWritable::deserializeBinary(ReadBuffer &, MergeTreeIndexVersion)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Deserialization of MergeTreeIndexGranuleTextWritable is not implemented");
}

MergeTreeIndexTextGranuleBuilder::MergeTreeIndexTextGranuleBuilder(const BloomFilterParameters & bloom_filter_params_, TokenExtractorPtr token_extractor_)
    : bloom_filter(bloom_filter_params_), token_extractor(token_extractor_), arena(std::make_unique<Arena>())
{
}

void MergeTreeIndexTextGranuleBuilder::addDocument(StringRef document)
{
    for (const auto & token : token_extractor->getTokensView(document.data, document.size))
    {
        bool inserted;
        TermsMap::LookupResult it;

        terms_map.emplace(token, it, inserted);

        if (inserted)
        {
            arena->insert(token.data(), token.size());
            bloom_filter.add(token.data(), token.size());
            it->getMapped() = &posting_lists.emplace_back();
        }

        it->getMapped()->add(current_row);
    }

    ++current_row;
}

std::unique_ptr<MergeTreeIndexGranuleTextWritable> MergeTreeIndexTextGranuleBuilder::build()
{
    std::vector<std::pair<StringRef, roaring::Roaring *>> sorted_values;
    sorted_values.reserve(terms_map.size());

    for (auto it = terms_map.begin(); it != terms_map.end(); ++it)
        sorted_values.emplace_back(it->getKey(), it->getMapped());

    std::sort(sorted_values.begin(), sorted_values.end(), [](const auto & a, const auto & b) { return a.first < b.first; });

    std::vector<StringRef> sorted_terms;
    std::vector<roaring::Roaring> sorted_posting_lists;

    sorted_terms.reserve(sorted_values.size());
    sorted_posting_lists.reserve(sorted_values.size());

    for (const auto & [token, posting_list] : sorted_values)
    {
        sorted_terms.emplace_back(token);
        sorted_posting_lists.emplace_back(std::move(*posting_list));
    }

    return std::make_unique<MergeTreeIndexGranuleTextWritable>(
        std::move(bloom_filter),
        std::move(sorted_terms),
        std::move(sorted_posting_lists),
        std::move(arena));
}

MergeTreeIndexAggregatorText::MergeTreeIndexAggregatorText(String index_column_name_, BloomFilterParameters bloom_filter_params_, TokenExtractorPtr token_extractor_)
    : index_column_name(std::move(index_column_name_)), bloom_filter_params(std::move(bloom_filter_params_)), token_extractor(token_extractor_), granule_builder(bloom_filter_params)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorText::getGranuleAndReset()
{
    auto granule = granule_builder->build();
    granule_builder.emplace(bloom_filter_params, token_extractor);
    return granule;
}

void MergeTreeIndexAggregatorText::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The provided position is not less than the number of block rows. "
                "Position: {}, Block rows: {}.", *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (rows_read == 0)
        return;

    size_t current_position = *pos;
    const auto & index_column = block.getByName(index_column_name).column;

    for (size_t i = 0; i < rows_read; ++i)
    {
        auto ref = index_column->getDataAt(current_position + i);
        granule_builder->addDocument(ref);
    }

    *pos += rows_read;
}

}
