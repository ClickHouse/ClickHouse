#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnsCommon.h>


namespace DB
{

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
    const IColumn::Filter * data = nullptr; /// Pointer to filter when it is not always true or always false.
    ColumnPtr data_holder;                  /// If new column was generated, it will be owned by holder.

    explicit FilterDescription(const IColumn & column);

    ColumnPtr filter(const IColumn & column, ssize_t result_size_hint) const override { return column.filter(*data, result_size_hint); }
    size_t countBytesInFilter() const override { return DB::countBytesInFilter(*data); }
};

struct SparseFilterDescription final : public IFilterDescription
{
    const IColumn * filter_indices = nullptr;
    explicit SparseFilterDescription(const IColumn & column);

    ColumnPtr filter(const IColumn & column, ssize_t) const override { return column.index(*filter_indices, 0); }
    size_t countBytesInFilter() const override { return filter_indices->size(); }
};

struct ColumnWithTypeAndName;

}
