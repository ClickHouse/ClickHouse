#pragma once
#include <Core/Types.h>

namespace DB
{

class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

String getReplicatedDatabaseShardName(const DatabasePtr & database);
String getReplicatedDatabaseReplicaName(const DatabasePtr & database);

}
