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
        throw Exception("MergeTreeIndexFactory: the Index creator name '" + index_type + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}
void MergeTreeIndexFactory::registerValidator(const std::string & index_type, Validator validator)
{
    if (!validators.emplace(index_type, std::move(validator)).second)
        throw Exception("MergeTreeIndexFactory: the Index validator name '" + index_type + "' is not unique", ErrorCodes::LOGICAL_ERROR);
}


MergeTreeIndexPtr MergeTreeIndexFactory::get(
    const IndexDescription & index) const
{
    auto it = creators.find(index.type);
    if (it == creators.end())
        throw Exception(
                "Unknown Index type '" + index.type + "'. Available index types: " +
                std::accumulate(creators.cbegin(), creators.cend(), std::string{},
                        [] (auto && left, const auto & right) -> std::string
                        {
                            if (left.empty())
                                return right.first;
                            else
                                return left + ", " + right.first;
                        }),
                ErrorCodes::INCORRECT_QUERY);

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
    auto it = validators.find(index.type);
    if (it == validators.end())
        throw Exception(
            "Unknown Index type '" + index.type + "'. Available index types: "
                + std::accumulate(
                    validators.cbegin(),
                    validators.cend(),
                    std::string{},
                    [](auto && left, const auto & right) -> std::string
                    {
                        if (left.empty())
                            return right.first;
                        else
                            return left + ", " + right.first;
                    }),
            ErrorCodes::INCORRECT_QUERY);

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
}

MergeTreeIndexFactory & MergeTreeIndexFactory::instance()
{
    static MergeTreeIndexFactory instance;
    return instance;
}

}
