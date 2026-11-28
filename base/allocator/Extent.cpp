#include <allocator/Extent.h>

namespace jemalloc
{

/// jemalloc: ph_gen(, edata_avail, edata_t, avail_link, edata_esnead_comp)
template class PairingHeap<Extent, &Extent::available_link, ExtentStructSerialNumberAndAddressCompare>;

/// jemalloc: ph_gen(, edata_heap, edata_t, heap_link, edata_snad_comp)
template class PairingHeap<Extent, &Extent::heap_link, ExtentSerialNumberAndAddressCompare>;

}
