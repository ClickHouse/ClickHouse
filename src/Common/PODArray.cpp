#include <Common/PODArray.h>

namespace DB
{

/// Used for left padding of PODArray when empty
const char empty_pod_array[empty_pod_array_size]{};

template class PODArray<UInt8, 4096, Allocator<false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;
template class PODArray<UInt16, 4096, Allocator<false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;
template class PODArray<UInt32, 4096, Allocator<false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;
template class PODArray<UInt64, 4096, Allocator<false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;

template class PODArray<Int8, 4096, Allocator<false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;
template class PODArray<Int16, 4096, Allocator<false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;
template class PODArray<Int32, 4096, Allocator<false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;
template class PODArray<Int64, 4096, Allocator<false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD>;

template class PODArray<UInt8, 4096, Allocator<false>, 0, 0>;
template class PODArray<UInt16, 4096, Allocator<false>, 0, 0>;
template class PODArray<UInt32, 4096, Allocator<false>, 0, 0>;
template class PODArray<UInt64, 4096, Allocator<false>, 0, 0>;

template class PODArray<Int8, 4096, Allocator<false>, 0, 0>;
template class PODArray<Int16, 4096, Allocator<false>, 0, 0>;
template class PODArray<Int32, 4096, Allocator<false>, 0, 0>;
template class PODArray<Int64, 4096, Allocator<false>, 0, 0>;
}
