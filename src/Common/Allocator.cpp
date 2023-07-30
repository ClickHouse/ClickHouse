#include "Allocator.h"


/// Constant is chosen almost arbitrarily, what I observed is 128KB is too small, 1MB is almost indistinguishable from 64MB and 1GB is too large.
extern const size_t POPULATE_THRESHOLD = 16 * 1024 * 1024;

template class Allocator<false, false>;
template class Allocator<true, false>;
template class Allocator<false, true>;
template class Allocator<true, true>;
