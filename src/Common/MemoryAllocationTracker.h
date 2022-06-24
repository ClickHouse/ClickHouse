#include <Common/PODArray_fwd.h>

namespace MemoryAllocationTracker
{

void enable_alocation_tracker(bool enable);

void track_alloc(void * ptr, std::size_t size);
void track_free(void * ptr, std::size_t size);

void dump_allocations(DB::PaddedPODArray<UInt64> & values, DB::PaddedPODArray<UInt64> & offsets, DB::PaddedPODArray<UInt64> & bytes);

}
