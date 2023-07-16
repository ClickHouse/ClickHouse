#include "StorageSystemContributors.h"

#include <algorithm>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/thread_local_rng.h>


extern size_t auto_contributors_size;
extern std::tuple<const char *, const char *, size_t> auto_contributors[];

namespace DB
{
NamesAndTypesList StorageSystemContributors::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"email", std::make_shared<DataTypeString>()},
        {"commits", std::make_shared<DataTypeUInt32>()},
    };
}

void StorageSystemContributors::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    std::vector<std::tuple<const char *, const char *, size_t>> contributors;
    for (size_t i = 0; i < auto_contributors_size; ++i)
    {
        contributors.emplace_back(auto_contributors[i]);
    }

    std::shuffle(contributors.begin(), contributors.end(), thread_local_rng);

    for (const auto & item : contributors)
    {
        res_columns[0]->insert(String{std::get<0>(item)});
        res_columns[1]->insert(String{std::get<1>(item)});
        res_columns[2]->insert(std::get<2>(item));
    }
}
}
