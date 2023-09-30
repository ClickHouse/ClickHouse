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
                            else
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
                        else
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

    registerCreator("ngrambf_v1", bloomFilterIndexCreator);
    registerValidator("ngrambf_v1", bloomFilterIndexValidator);

    registerCreator("tokenbf_v1", bloomFilterIndexCreator);
    registerValidator("tokenbf_v1", bloomFilterIndexValidator);

    registerCreator("bloom_filter", bloomFilterIndexCreatorNew);
    registerValidator("bloom_filter", bloomFilterIndexValidatorNew);

    registerCreator("hypothesis", hypothesisIndexCreator);
    registerValidator("hypothesis", hypothesisIndexValidator);

#ifdef ENABLE_ANNOY
    registerCreator("annoy", annoyIndexCreator);
    registerValidator("annoy", annoyIndexValidator);
#endif

#ifdef ENABLE_USEARCH
    registerCreator("usearch", usearchIndexCreator);
    registerValidator("usearch", usearchIndexValidator);
#endif

    registerCreator("inverted", invertedIndexCreator);
    registerValidator("inverted", invertedIndexValidator);

}

MergeTreeIndexFactory & MergeTreeIndexFactory::instance()
{
    static MergeTreeIndexFactory instance;
    return instance;
}

}
