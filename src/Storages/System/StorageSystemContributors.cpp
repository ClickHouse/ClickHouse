#include "StorageSystemContributors.h"

#include <algorithm>
#include <DataTypes/DataTypeString.h>
#include <Common/thread_local_rng.h>


extern const char * auto_contributors[];

namespace DB
{
ColumnsDescription StorageSystemContributors::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Contributor (author) name from git log."},
    };
}

void StorageSystemContributors::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    std::vector<const char *> contributors;
    for (auto * it = auto_contributors; *it; ++it)
        contributors.emplace_back(*it);

    std::shuffle(contributors.begin(), contributors.end(), thread_local_rng);

    for (auto & it : contributors)
        res_columns[0]->insert(String(it));
}
}
