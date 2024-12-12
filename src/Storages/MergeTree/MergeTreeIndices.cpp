#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <numeric>

#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

void MergeTreeIndexFactory::registerCreator(const std::string & index_type, Creator creator)
{
    if (!creators.emplace(index_type, std::move(creator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeIndexFactory: the Index creator name '{}' is not unique",
                        index_type);
}
void MergeTreeIndexFactory::registerValidator(const std::string & index_type, Validator validator)
{
    if (!validators.emplace(index_type, std::move(validator)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeIndexFactory: the Index validator name '{}' is not unique", index_type);
}


MergeTreeIndexPtr MergeTreeIndexFactory::get(
    const IndexDescription & index) const
{
    auto it = creators.find(index.type);
    if (it == creators.end())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Unknown Index type '{}'. Available index types: {}", index.type,
                std::accumulate(creators.cbegin(), creators.cend(), std::string{},
                        [] (auto && left, const auto & right) -> std::string
                        {
                            if (left.empty())
                                return right.first;
                            return left + ", " + right.first;
                        })
                );
    }

    return it->second(index);
}


MergeTreeIndices MergeTreeIndexFactory::getMany(const std::vector<IndexDescription> & indices) const
{
    MergeTreeIndices result;
    for (const auto & index : indices)
        result.emplace_back(get(index));
    return result;
}

void MergeTreeIndexFactory::validate(const IndexDescription & index, bool attach) const
{
    /// Do not allow constant and non-deterministic expressions.
    /// Do not throw on attach for compatibility.
    if (!attach)
    {
        if (index.expression->hasArrayJoin())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Secondary index '{}' cannot contain array joins", index.name);

        try
        {
            index.expression->assertDeterministic();
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("for secondary index '{}'", index.name));
            throw;
        }

        for (const auto & elem : index.sample_block)
            if (elem.column && (isColumnConst(*elem.column) || elem.column->isDummy()))
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Secondary index '{}' cannot contain constants", index.name);
    }

    auto it = validators.find(index.type);
    if (it == validators.end())
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Unknown Index type '{}'. Available index types: {}", index.type,
                std::accumulate(
                    validators.cbegin(),
                    validators.cend(),
                    std::string{},
                    [](auto && left, const auto & right) -> std::string
                    {
                        if (left.empty())
                            return right.first;
                        return left + ", " + right.first;
                    })
            );
    }

    it->second(index, attach);
}

MergeTreeIndexFactory::MergeTreeIndexFactory()
{
    registerCreator("minmax", minmaxIndexCreator);
    registerValidator("minmax", minmaxIndexValidator);

    registerCreator("set", setIndexCreator);
    registerValidator("set", setIndexValidator);

    registerCreator("ngrambf_v1", bloomFilterIndexTextCreator);
    registerValidator("ngrambf_v1", bloomFilterIndexTextValidator);

    registerCreator("tokenbf_v1", bloomFilterIndexTextCreator);
    registerValidator("tokenbf_v1", bloomFilterIndexTextValidator);

    registerCreator("bloom_filter", bloomFilterIndexCreator);
    registerValidator("bloom_filter", bloomFilterIndexValidator);

    registerCreator("hypothesis", hypothesisIndexCreator);

    registerValidator("hypothesis", hypothesisIndexValidator);

#if USE_USEARCH
    registerCreator("vector_similarity", vectorSimilarityIndexCreator);
    registerValidator("vector_similarity", vectorSimilarityIndexValidator);
#endif
    /// ------
    /// TODO: remove this block at the end of 2024.
    /// Index types 'annoy' and 'usearch' are no longer supported as of June 2024. Their successor is index type 'vector_similarity'.
    /// To support loading tables with old indexes during a transition period, register dummy indexes which allow load/attaching but
    /// throw an exception when the user attempts to use them.
    registerCreator("annoy", legacyVectorSimilarityIndexCreator);
    registerValidator("annoy", legacyVectorSimilarityIndexValidator);
    registerCreator("usearch", legacyVectorSimilarityIndexCreator);
    registerValidator("usearch", legacyVectorSimilarityIndexValidator);
    /// ------

    registerCreator("inverted", fullTextIndexCreator);
    registerValidator("inverted", fullTextIndexValidator);

    /// ------
    /// TODO: remove this block at the end of 2024.
    /// Index type 'inverted' was renamed to 'full_text' in May 2024.
    /// To support loading tables with old indexes during a transition period, register full-text indexes under their old name.
    registerCreator("full_text", fullTextIndexCreator);
    registerValidator("full_text", fullTextIndexValidator);
    /// ------
}

MergeTreeIndexFactory & MergeTreeIndexFactory::instance()
{
    static MergeTreeIndexFactory instance;
    return instance;
}

}
