#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>

#include <type_traits>

#if __SSE2__
    #define LIBDIVIDE_USE_SSE2 1
#endif

#include <libdivide.h>


namespace DB
{

template <typename T>
IColumn::Selector createBlockSelector(
    const IColumn & column,
    const std::vector<UInt64> & slots)
{
    const auto total_weight = slots.size();
    size_t num_rows = column.size();
    IColumn::Selector selector(num_rows);

    /** Modulo of division of negative numbers to positive number in C++11 is negative (so called truncated division).
      * This is not suitable for our task. So we will process signed numbers as unsigned.
      * It is not near like remainder of division, but is suitable for our task.
      */
    using UnsignedT = std::make_unsigned_t<T>;

    /// const columns contain only one value, therefore we do not need to read it at every iteration
    if (column.isColumnConst())
    {
        const auto data = static_cast<const ColumnConst &>(column).getValue<T>();
        const auto shard_num = slots[static_cast<UnsignedT>(data) % total_weight];
        selector.assign(num_rows, shard_num);
    }
    else
    {
        /// libdivide support only UInt32 and UInt64.
        using TUInt32Or64 = std::conditional_t<sizeof(UnsignedT) <= 4, UInt32, UInt64>;

        libdivide::divider<TUInt32Or64> divider(total_weight);

        const auto & data = typeid_cast<const ColumnVector<T> &>(column).getData();

        for (size_t i = 0; i < num_rows; ++i)
            selector[i] = slots[static_cast<TUInt32Or64>(data[i]) - (static_cast<TUInt32Or64>(data[i]) / divider) * total_weight];
    }

    return selector;
}


/// Explicit instantiations to avoid code bloat in headers.
template IColumn::Selector createBlockSelector<UInt8>(const IColumn & column, const std::vector<UInt64> & slots);
template IColumn::Selector createBlockSelector<UInt16>(const IColumn & column, const std::vector<UInt64> & slots);
template IColumn::Selector createBlockSelector<UInt32>(const IColumn & column, const std::vector<UInt64> & slots);
template IColumn::Selector createBlockSelector<UInt64>(const IColumn & column, const std::vector<UInt64> & slots);
template IColumn::Selector createBlockSelector<Int8>(const IColumn & column, const std::vector<UInt64> & slots);
template IColumn::Selector createBlockSelector<Int16>(const IColumn & column, const std::vector<UInt64> & slots);
template IColumn::Selector createBlockSelector<Int32>(const IColumn & column, const std::vector<UInt64> & slots);
template IColumn::Selector createBlockSelector<Int64>(const IColumn & column, const std::vector<UInt64> & slots);

}
