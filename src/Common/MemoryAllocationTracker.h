#include <Common/PODArray_fwd.h>

namespace MemoryAllocationTracker
{

void enable_allocation_tracker();
void disable_allocation_tracker();

void track_alloc(void * ptr, std::size_t size);
void track_free(void * ptr, std::size_t size);

void dump_allocations(DB::PaddedPODArray<UInt64> & values, DB::PaddedPODArray<UInt64> & offsets, DB::PaddedPODArray<UInt64> & bytes);
void dump_allocations_tree(DB::PaddedPODArray<UInt8> & chars, DB::PaddedPODArray<UInt64> & offsets, size_t max_depth, size_t max_bytes);

}
