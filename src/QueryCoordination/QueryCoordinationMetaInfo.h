#pragma once

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/StorageID.h>

namespace DB
{

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
