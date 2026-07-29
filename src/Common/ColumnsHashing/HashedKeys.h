#pragma once

#include <Common/PODArray_fwd.h>
#include <Core/Types.h>
#include <base/types.h>

namespace DB::ColumnsHashing
{

/// The keys of `HashMethodHashed`, one per row of a block: a SipHash128 digest of all the key
/// columns is the key itself (the hash table hash is taken on top of it). Shared between all
/// consumers of the block.
using HashedKeysPtr = std::shared_ptr<const PaddedPODArray<UInt128>>;

}
