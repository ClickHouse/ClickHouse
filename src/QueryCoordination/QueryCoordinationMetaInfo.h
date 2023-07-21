#pragma once

#include <Interpreters/StorageID.h>

namespace DB
{

using String = std::string;

class QueryCoordinationMetaInfo
{
public:
    String cluster_name;
    std::vector<StorageID> storages;
    std::vector<String> sharding_keys;
};

}

