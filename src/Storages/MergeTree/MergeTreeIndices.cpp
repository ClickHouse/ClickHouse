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

void MergeTreeIndexFactory::registerIndex(const std::string & index_type, Creator creator)
{
    if (!indexes.emplace(index_type, std::move(creator)).second)
        throw Exception("MergeTreeIndexFactory: the Index creator name '" + index_type + "' is not unique",
                        ErrorCodes::LOGICAL_ERROR);
}

std::unique_ptr<IMergeTreeIndex> MergeTreeIndexFactory::get(
    const StorageMetadataSkipIndexField & index, bool attach) const
{
    auto it = indexes.find(index.type);
    if (it == indexes.end())
        throw Exception(
                "Unknown Index type '" + index.type + "'. Available index types: " +
                std::accumulate(indexes.cbegin(), indexes.cend(), std::string{},
                        [] (auto && left, const auto & right) -> std::string
                        {
                            if (left.empty())
                                return right.first;
                            else
                                return left + ", " + right.first;
                        }),
                ErrorCodes::INCORRECT_QUERY);

    return it->second(index, attach);
}

MergeTreeIndexFactory::MergeTreeIndexFactory()
{
    registerIndex("minmax", minmaxIndexCreator);
    registerIndex("set", setIndexCreator);
    registerIndex("ngrambf_v1", bloomFilterIndexCreator);
    registerIndex("tokenbf_v1", bloomFilterIndexCreator);
    registerIndex("bloom_filter", bloomFilterIndexCreatorNew);
}

MergeTreeIndexFactory & MergeTreeIndexFactory::instance()
{
    static MergeTreeIndexFactory instance;
    return instance;
}

}
