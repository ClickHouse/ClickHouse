#pragma once

#include <Common/PODArray.h>
#include <string_view>
#include <memory>

namespace DB
{

/// Buffer holding serialized GROUP BY keys for all rows in a chunk.
/// Used by sharded aggregation to serialize keys once (during scatter)
/// and reuse them during aggregation, avoiding re-serialization.
struct SerializedKeyBuffer
{
    PaddedPODArray<char> data;       /// contiguous serialized key bytes for all rows
    PaddedPODArray<UInt64> offsets;  /// offsets[i] = byte start of row i's key
                                     /// key i = string_view(data + offsets[i], offsets[i+1] - offsets[i])
                                     /// has row_count + 1 entries (last = total size)

    std::string_view getKey(size_t row) const
    {
        return {data.data() + offsets[row], offsets[row + 1] - offsets[row]};
    }
};
using SerializedKeyBufferPtr = std::shared_ptr<const SerializedKeyBuffer>;

}
