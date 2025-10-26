#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/PODArray_fwd.h>

namespace DB
{

template <class> class ColumnVector;
using ColumnUInt64 = ColumnVector<UInt64>;

using IColumnFilter = PaddedPODArray<UInt8>;
bool tryConvertAnyColumnToBool(const IColumn & column, IColumnFilter & res);

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

struct FilterDescription final : public IFilterDescription
{
    const IColumnFilter * data = nullptr;
    ColumnPtr data_holder;

    explicit FilterDescription(const IColumn & column);

    ColumnPtr filter(const IColumn & column, ssize_t result_size_hint) const override;
    size_t countBytesInFilter() const override;

    /// Takes a filter column that may be sparse or const or low-cardinality or nullable or wider
    /// than 8 bits, etc. Returns a ColumnUInt8.
    static ColumnPtr preprocessFilterColumn(ColumnPtr column);
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
