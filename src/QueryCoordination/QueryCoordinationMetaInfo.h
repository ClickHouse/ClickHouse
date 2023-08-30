#pragma once

#include <Interpreters/StorageID.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

using String = std::string;

class QueryCoordinationMetaInfo
{
public:
    void write(WriteBuffer & out) const;

    void read(ReadBuffer & in);

    String toString() const;

    String cluster_name;
    std::vector<StorageID> storages;
    std::vector<String> sharding_keys;
};

}
