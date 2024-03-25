#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>


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
    /// has_one can be pre-compute during creating the filter description in some cases
    Int64 has_one = -1;
    virtual ColumnPtr filter(const IColumn & column, ssize_t result_size_hint) const = 0;
    virtual void filterInPlace(IColumn & column) const;
    virtual size_t countBytesInFilter() const = 0;
    virtual ~IFilterDescription() = default;
    bool hasOne() { return has_one >= 0 ? has_one : hasOneImpl();}
protected:
    /// Calculate if filter has a non-zero from the filter values, may update has_one
    virtual bool hasOneImpl() = 0;
};

/// Obtain a filter from non constant Column, that may have type: UInt8, Nullable(UInt8).
struct FilterDescription final : public IFilterDescription
{
    const IColumn::Filter * data = nullptr; /// Pointer to filter when it is not always true or always false.
    ColumnPtr data_holder;                  /// If new column was generated, it will be owned by holder.

    explicit FilterDescription(const IColumn & column);

    ColumnPtr filter(const IColumn & column, ssize_t result_size_hint) const override { return column.filter(*data, result_size_hint); }
    void filterInPlace(IColumn & column) const override;
    size_t countBytesInFilter() const override;

protected:
    bool hasOneImpl() override { return countBytesInFilter() > 0; }

private:
    void lazyInitializeForFilterInPlace() const;

    mutable std::atomic<bool> initialized_for_filter_in_place = false;

    /// Where to start filterInPlace, assuming that rows between [0, start) should all output in the result. Only used in filterInPlace.
    mutable size_t start = 0;
    /// For rows between [start, end), which indices should be output in the result. Only used in filterInPlace.
    mutable PaddedPODArray<UInt64> indexes;
};

struct SparseFilterDescription final : public IFilterDescription
{
    const ColumnUInt64 * filter_indices = nullptr;
    explicit SparseFilterDescription(const IColumn & column);

    ColumnPtr filter(const IColumn & column, ssize_t) const override { return column.index(*filter_indices, 0); }
    void filterInPlace(IColumn & column) const override { return column.filterInPlace(filter_indices->getData(), 0); }
    size_t countBytesInFilter() const override { return filter_indices->size(); }
protected:
    bool hasOneImpl() override { return !filter_indices->empty(); }
};

struct ColumnWithTypeAndName;

}
