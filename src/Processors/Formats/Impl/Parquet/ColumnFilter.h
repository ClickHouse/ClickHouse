#pragma once
#include <iostream>
#include <unordered_set>
#include <base/types.h>
#include <boost/dynamic_bitset.hpp>
#include <Common/Exception.h>

#include <Interpreters/ActionsDAG.h>

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
    virtual ColumnFilterKind kind() const { return kind_; }
    virtual bool testNull() const { return null_allowed; }
    virtual bool testNotNull() const { return true; }
    virtual bool testInt64(Int64) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testInt64 not implemented"); }
    virtual bool testFloat32(Float32) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testFloat32 not implemented"); }
    virtual bool testFloat64(Float64) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testFloat64 not implemented"); }
    virtual bool testBool(bool) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testBool not implemented"); }
    virtual bool testString(const String & /*value*/) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testString not implemented"); }
    virtual bool testInt64Range(Int64, Int64) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testInt64Range not implemented"); }
    virtual bool testFloat32Range(Float32, Float32) const
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testFloat32Range not implemented");
    }
    virtual bool testFloat64Range(Float64, Float64) const
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testFloat64Range not implemented");
    }
    virtual void testInt64Values(RowSet & row_set, size_t offset, size_t len, const Int64 * data) const
    {
        for (size_t i = offset; i < offset+len; ++i)
        {
            row_set.set(i, testInt64(data[i]));
        }
    }

    virtual ColumnFilterPtr merge(const ColumnFilter * /*filter*/) const
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "merge not implemented");
    }

protected:
    ColumnFilterKind kind_;
    bool null_allowed;
};

class IsNullFilter : public ColumnFilter
{
public:
    IsNullFilter() : ColumnFilter(IsNull, true) { }
    bool testNotNull() const override { return false; }
};

class IsNotNullFilter : public ColumnFilter
{
public:
    IsNotNullFilter() : ColumnFilter(IsNotNull, false) { }
};

class AlwaysTrueFilter : public ColumnFilter
{
public:
    AlwaysTrueFilter() : ColumnFilter(AlwaysTrue, true) { }
    bool testNull() const override { return true; }
    bool testNotNull() const override { return true; }
    bool testInt64(Int64) const override { return true; }
    bool testFloat32(Float32) const override { return true; }
    bool testFloat64(Float64) const override { return true; }
    bool testBool(bool) const override { return true; }
    bool testInt64Range(Int64, Int64) const override { return true; }
    bool testFloat32Range(Float32, Float32) const override { return true; }
    bool testFloat64Range(Float64, Float64) const override { return true; }
    void testInt64Values(RowSet & set, size_t, size_t, const Int64 *) const override { set.setAllTrue(); }
    bool testString(const String & /*value*/) const override { return true; }
    ColumnFilterPtr merge(const ColumnFilter * /*filter*/) const override { return std::make_shared<AlwaysTrueFilter>(); }
};

class AlwaysFalseFilter : public ColumnFilter
{
public:
    AlwaysFalseFilter() : ColumnFilter(AlwaysFalse, false) { }
    bool testNull() const override { return false; }
    bool testNotNull() const override { return false; }
    bool testInt64(Int64) const override { return false; }
    bool testFloat32(Float32) const override { return false; }
    bool testFloat64(Float64) const override { return false; }
    bool testBool(bool) const override { return true; }
    bool testInt64Range(Int64, Int64) const override { return false; }
    bool testFloat32Range(Float32, Float32) const override { return false; }
    bool testFloat64Range(Float64, Float64) const override { return false; }
    void testInt64Values(RowSet & set, size_t, size_t, const Int64 *) const override { set.setAllFalse(); }
    bool testString(const String & /*value*/) const override { return false; }
    ColumnFilterPtr merge(const ColumnFilter * /*filter*/) const override { return std::make_shared<AlwaysFalseFilter>(); }
};

class Int64RangeFilter : public ColumnFilter
{
public:
    static ColumnFilterPtr create(const ActionsDAG::Node & node);
    explicit Int64RangeFilter(const Int64 min_, const Int64 max_, bool null_allowed_)
        : ColumnFilter(Int64Range, null_allowed_), max(max_), min(min_), is_single_value(max == min)
    {
    }
    ~Int64RangeFilter() override = default;
    bool testInt64(Int64 int64) const override { return int64 >= min && int64 <= max; }
    bool testInt64Range(Int64 lower, Int64 upper) const override { return min >= lower && max <= upper; }
    void testInt64Values(RowSet & row_set, size_t offset, size_t len, const Int64 * data) const override;
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;

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

    ByteValuesFilter(const std::unordered_set<String> & values_, bool null_allowed_)
        : ColumnFilter(ByteValues, null_allowed_), values(values_)
    {
        std::ranges::for_each(values_, [&](const String & value) { lengths.insert(value.size()); });
    }

    bool testString(const String & value) const override { return lengths.contains(value.size()) && values.contains(value); }
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;

private:
    std::unordered_set<size_t> lengths;
    std::unordered_set<String> values;
};

class Int64MultiRangeFilter : public ColumnFilter
{
public:
    Int64MultiRangeFilter(const std::vector<ColumnFilterPtr> & ranges_, bool null_allowed_)
        : ColumnFilter(Int64MultiRange, null_allowed_), ranges(ranges_)
    {
    }

private:
    std::vector<ColumnFilterPtr> ranges;
    std::vector<Int64> lower_bounds;
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
concept is_floating_point = std::is_same_v<T, double> || std::is_same_v<T, float>;

template <is_floating_point T>
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

    bool testFloat32(Float32 value) const override { return testFloatingPoint(value); }
    bool testFloat64(Float64 value) const override { return testFloatingPoint(value); }
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;

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
