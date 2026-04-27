#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/PODArray_fwd.h>

namespace DB
{

template <class> class ColumnVector;
using ColumnUInt64 = ColumnVector<UInt64>;

using IColumnFilter = PaddedPODArray<UInt8>;

/// Support methods for implementation of WHERE, PREWHERE and HAVING.


/// Analyze if the column for filter is constant thus filter is always false or always true.
struct ConstantFilterDescription
{
    bool always_false = false;
    bool always_true = false;

    ConstantFilterDescription() = default;
    explicit ConstantFilterDescription(const IColumn & column);
};

struct IFilterDescription
{
    virtual ColumnPtr filter(const IColumn & column, ssize_t result_size_hint) const = 0;
    virtual size_t countBytesInFilter() const = 0;
    virtual ~IFilterDescription() = default;
};

/// Obtain a filter from non constant Column, that may have type: UInt8, Nullable(UInt8).
struct FilterDescription final : public IFilterDescription
{
    const IColumnFilter * data = nullptr; /// Pointer to filter when it is not always true or always false.
    ColumnPtr data_holder;                  /// If new column was generated, it will be owned by holder.

    explicit FilterDescription(const IColumn & column);

    ColumnPtr filter(const IColumn & column, ssize_t result_size_hint) const override;
    size_t countBytesInFilter() const override;
};

struct SparseFilterDescription final : public IFilterDescription
{
    const ColumnUInt64 * filter_indices = nullptr;
    explicit SparseFilterDescription(const IColumn & column);

    ColumnPtr filter(const IColumn & column, ssize_t) const override;
    size_t countBytesInFilter() const override;
};

struct ColumnWithTypeAndName;

}
