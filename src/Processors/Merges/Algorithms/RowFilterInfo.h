#pragma once

#include <Columns/FilterDescription.h>
#include <Common/PODArray.h>
#include <Processors/Chunk.h>

namespace DB
{

/// Rows a merge must not emit, computed upstream and carried beside the data
struct RowFilterInfo : public ChunkInfoCloneable<RowFilterInfo>
{
    explicit RowFilterInfo(IColumnFilter mask_) : mask(std::move(mask_)) {}

    /// `IColumnFilter` is move-only, so `clone`'s deep copy has to be written by hand.
    RowFilterInfo(const RowFilterInfo & other) { mask.assign(other.mask); }

    /// Not supported: nothing combines two chunks that each carry a mask.
    Ptr merge(const Ptr &) const override;

    IColumnFilter mask;
};

/// The chunk's row filter, or nullptr when it carries none - a merge filters exactly the chunks
/// that arrive with a mask. A debug build asserts that the mask covers the chunk.
const IColumnFilter * getRowFilterMask(const Chunk & chunk);

}
