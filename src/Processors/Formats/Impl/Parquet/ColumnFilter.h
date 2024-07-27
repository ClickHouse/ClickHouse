#pragma once
#include <base/types.h>
#include <boost/dynamic_bitset.hpp>
#include <Common/Exception.h>
#include <unordered_set>
#include <iostream>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int PARQUET_EXCEPTION;
}
class RowSet
{
public:
    explicit RowSet(size_t max_rows_) : max_rows(max_rows_) { bitset.resize(max_rows, true); }

    void set(size_t i, bool value) { bitset.set(i, value); }
    bool get(size_t i) const { return bitset.test(i); }
    size_t totalRows() const { return max_rows; }
    bool none() const { return bitset.none(); }
    bool all() const { return bitset.all(); }
    bool any() const { return bitset.any(); }
    void setAllTrue() { bitset.resize(max_rows, true); }
    void setAllFalse() { bitset.resize(max_rows, false); }

private:
    size_t max_rows = 0;
    boost::dynamic_bitset<> bitset;
};

class ColumnFilter;
using ColumnFilterPtr = std::shared_ptr<ColumnFilter>;

enum ColumnFilterKind
{
    Unknown,
    AlwaysTrue,
    AlwaysFalse,
    IsNull,
    IsNotNull,
    Int64Range,
    Int64MultiRange,
    Int64In,
    FloatRange,
    DoubleRange,
    ByteValues
};


class ColumnFilter
{
public:
    ColumnFilter(ColumnFilterKind kind, bool null_allowed_) : kind_(kind), null_allowed(null_allowed_) { }
    virtual ~ColumnFilter() = default;
    virtual ColumnFilterKind kind() { return kind_; }
    virtual bool testNull() { return null_allowed; }
    virtual bool testNotNull() { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testNotNull not implemented"); }
    virtual bool testInt64(Int64) { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testInt64 not implemented"); }
    virtual bool testFloat32(Float32) { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testFloat32 not implemented"); }
    virtual bool testFloat64(Float64) { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testFloat64 not implemented"); }
    virtual bool testBool(bool) { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testBool not implemented"); }
    virtual bool testString(const String & /*value*/) { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testString not implemented"); }
    virtual bool testInt64Range(Int64, Int64) { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testInt64Range not implemented"); }
    virtual bool testFloat32Range(Float32, Float32)
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testFloat32Range not implemented");
    }
    virtual bool testFloat64Range(Float64, Float64)
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testFloat64Range not implemented");
    }
    virtual void testInt64Values(RowSet & /*row_set*/, size_t /*offset*/, size_t /*len*/, const Int64 * /*data*/)
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testInt64Values not implemented");
    }

protected:
    ColumnFilterKind kind_;
    bool null_allowed;
};

class AlwaysTrueFilter : public ColumnFilter
{
public:
    ColumnFilterKind kind() override { return AlwaysTrue; }
    bool testNull() override { return true; }
    bool testNotNull() override { return true; }
    bool testInt64(Int64) override { return true; }
    bool testFloat32(Float32) override { return true; }
    bool testFloat64(Float64) override { return true; }
    bool testBool(bool) override { return true; }
    bool testInt64Range(Int64, Int64) override { return true; }
    bool testFloat32Range(Float32, Float32) override { return true; }
    bool testFloat64Range(Float64, Float64) override { return true; }
    void testInt64Values(RowSet & set, size_t, size_t, const Int64 *) override { set.setAllTrue(); }
};

class Int64RangeFilter : public ColumnFilter
{
public:
    explicit Int64RangeFilter(const Int64 min_, const Int64 max_, bool null_allowed_)
        : ColumnFilter(Int64Range, null_allowed_), max(max_), min(min_), is_single_value(max == min)
    {
    }
    ~Int64RangeFilter() override = default;
    bool testInt64(Int64 int64) override { return int64 >= min && int64 <= max; }
    bool testInt64Range(Int64 lower, Int64 upper) override { return min >= lower && max <= upper; }
    void testInt64Values(RowSet & row_set, size_t offset, size_t len, const Int64 * data) override;

private:
    const Int64 max;
    const Int64 min;
    bool is_single_value [[maybe_unused]];
};
class ByteValuesFilter : public ColumnFilter
{
public:
    ByteValuesFilter(const std::vector<String> & values_, bool null_allowed_)
        : ColumnFilter(ByteValues, null_allowed_), values(values_.begin(), values_.end())
    {
        std::ranges::for_each(values_, [&](const String & value) { lengths.insert(value.size()); });
    }

    bool testString(const String & value) override
    {
        return lengths.contains(value.size()) && values.contains(value);
    }

private:
    std::unordered_set<size_t> lengths;
    std::unordered_set<String> values;
};


class AbstractRange : public ColumnFilter
{
public:
    bool lowerUnbounded() const { return lower_unbounded; }

    bool lowerExclusive() const { return lower_exclusive; }

    bool upperUnbounded() const { return upper_unbounded; }

    bool upperExclusive() const { return upper_exclusive; }

protected:
    AbstractRange(
        bool lower_unbounded_,
        bool lower_exclusive_,
        bool upper_unbounded_,
        bool upper_exclusive_,
        bool null_allowed_,
        ColumnFilterKind kind)
        : ColumnFilter(kind, null_allowed_)
        , lower_unbounded(lower_unbounded_)
        , lower_exclusive(lower_exclusive_)
        , upper_unbounded(upper_unbounded_)
        , upper_exclusive(upper_exclusive_)
    {
        if (!lower_unbounded && !upper_unbounded)
        {
            throw DB::Exception(ErrorCodes::PARQUET_EXCEPTION, "A range filter must have a lower or upper bound");
        }
    }

    const bool lower_unbounded;
    const bool lower_exclusive;
    const bool upper_unbounded;
    const bool upper_exclusive;
};

template <class T>
class FloatRangeFilter : public AbstractRange
{
public:
    FloatRangeFilter(
        const T min_, bool lowerUnbounded, bool lowerExclusive, const T max_, bool upperUnbounded, bool upperExclusive, bool nullAllowed)
        : AbstractRange(
              lowerUnbounded,
              lowerExclusive,
              upperUnbounded,
              upperExclusive,
              nullAllowed,
              (std::is_same_v<T, double>) ? ColumnFilterKind::DoubleRange : ColumnFilterKind::FloatRange)
        , min(min_)
        , max(max_)
    {
    }

    bool testFloat32(Float32 value) override { return testFloatingPoint(value); }
    bool testFloat64(Float64 value) override { return testFloatingPoint(value); }

private:
    bool testFloatingPoint(T value) const
    {
        if (std::isnan(value))
            return false;
        if (!lower_unbounded)
        {
            if (value < min)
                return false;
            if (lower_exclusive && min == value)
                return false;
        }
        if (!upper_unbounded)
        {
            if (value > max)
                return false;
            if (upper_exclusive && value == max)
                return false;
        }
        return true;
    }

    const T min;
    const T max;
};

}
