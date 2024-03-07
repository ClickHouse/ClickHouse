#include <AggregateFunctions/findNumeric.h>

namespace DB
{
#define INSTANTIATION(T) \
    template std::optional<T> findNumericMin(const T * __restrict ptr, size_t start, size_t end); \
    template std::optional<T> findNumericMinNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findNumericMinIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findNumericMax(const T * __restrict ptr, size_t start, size_t end); \
    template std::optional<T> findNumericMaxNotNull(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end); \
    template std::optional<T> findNumericMaxIf(const T * __restrict ptr, const UInt8 * __restrict condition_map, size_t start, size_t end);

FOR_BASIC_NUMERIC_TYPES(INSTANTIATION)
#undef INSTANTIATION
}
