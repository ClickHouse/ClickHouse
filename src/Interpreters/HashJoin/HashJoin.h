#pragma once

#include <Interpreters/HashJoin/HashJoinTypes.h>

namespace DB
{

/// The types of the hash join (`HashJoin::Type`, `HashJoin::MapsAll`, ...). The join itself is `PartitionedHashJoin`.
using HashJoin = HashJoinTypes;
}
