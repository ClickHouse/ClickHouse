#pragma once
#include <iostream>
#include <unordered_set>
#include <Columns/ColumnsCommon.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <base/types.h>
#include <boost/dynamic_bitset.hpp>
#include <Common/Exception.h>


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
    explicit RowSet(size_t max_rows_) : max_rows(max_rows_) { mask.resize_fill(max_rows, true); }

    inline void set(size_t i, bool value) { mask[i] = value; }
    inline bool get(size_t i) const { return mask[i]; }
    inline size_t totalRows() const { return max_rows; }
    bool none() const;
    bool all() const;
    bool any() const;
    void setAllTrue() { mask.resize(max_rows, true); }
    void setAllFalse() { mask.resize(max_rows, false); }
    size_t count() const { return std::reduce(mask.begin(), mask.end(), 0, [](bool a, bool b) { return a+b;}); }
    PaddedPODArray<bool> & maskReference() { return mask; }
    const PaddedPODArray<bool> & maskReference() const { return mask; }

private:
    size_t max_rows = 0;
    PaddedPODArray<bool> mask;
};
using OptionalRowSet = std::optional<RowSet>;

bool isConstantNode(const ActionsDAG::Node & node);
bool isCompareColumnWithConst(const ActionsDAG::Node & node);
const ActionsDAG::Node * getInputNode(const ActionsDAG::Node & node);
ActionsDAG::NodeRawConstPtrs getConstantNode(const ActionsDAG::Node & node);
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

class FilterHelper
{
public:
    template <typename T, typename S>
    static void filterPlainFixedData(const S * src, PaddedPODArray<T> & dst, const RowSet & row_set, size_t rows_to_read);
    template <typename T>
    static void
    gatherDictFixedValue(const PaddedPODArray<T> & dict, PaddedPODArray<T> & dst, const PaddedPODArray<Int32> & idx, size_t rows_to_read);
    template <typename T>
    static void filterDictFixedData(
        const PaddedPODArray<T> & dict,
        PaddedPODArray<T> & dst,
        const PaddedPODArray<Int32> & idx,
        const RowSet & row_set,
        size_t rows_to_read);
};

class ColumnFilter
{
protected:
    ColumnFilter(ColumnFilterKind kind, bool null_allowed_) : kind_(kind), null_allowed(null_allowed_) { }

public:
    virtual ~ColumnFilter() = default;
    virtual ColumnFilterKind kind() const { return kind_; }
    virtual bool testNull() const { return null_allowed; }
    virtual bool testNotNull() const { return true; }
    virtual bool testInt64(Int64) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testInt64 not implemented"); }
    virtual bool testInt32(Int32) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testInt32 not implemented"); }
    virtual bool testInt16(Int16) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testInt16 not implemented"); }
    virtual bool testFloat32(Float32) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testFloat32 not implemented"); }
    virtual bool testFloat64(Float64) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testFloat64 not implemented"); }
    virtual bool testBool(bool) const { throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testBool not implemented"); }
    virtual bool testString(const String & /*value*/) const
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "testString not implemented");
    }
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
        for (size_t i = offset; i < offset + len; ++i)
        {
            row_set.set(i, testInt64(data[i]));
        }
    }

    virtual void testInt32Values(RowSet & row_set, size_t offset, size_t len, const Int32 * data) const
    {
        for (size_t i = offset; i < offset + len; ++i)
        {
            row_set.set(i, testInt32(data[i]));
        }
    }

    virtual void testInt16Values(RowSet & row_set, size_t offset, size_t len, const Int16 * data) const
    {
        for (size_t i = offset; i < offset + len; ++i)
        {
            row_set.set(i, testInt16(data[i]));
        }
    }


    virtual ColumnFilterPtr merge(const ColumnFilter * /*filter*/) const
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "merge not implemented");
    }

    virtual ColumnFilterPtr clone(std::optional<bool> nullAllowed) const = 0;

    virtual String toString() const
    {
        return fmt::format("Filter({}, {})", magic_enum::enum_name(kind()), null_allowed ? "null allowed" : "null not allowed");
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
    ColumnFilterPtr clone(std::optional<bool>) const override { return std::make_shared<IsNullFilter>(); }
};

class IsNotNullFilter : public ColumnFilter
{
public:
    IsNotNullFilter() : ColumnFilter(IsNotNull, false) { }
    ColumnFilterPtr clone(std::optional<bool>) const override { return std::make_shared<IsNotNullFilter>(); }
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
    ColumnFilterPtr clone(std::optional<bool>) const override { return std::make_shared<AlwaysTrueFilter>(); }
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
    ColumnFilterPtr clone(std::optional<bool>) const override { return std::make_shared<AlwaysFalseFilter>(); }
};
using OptionalFilter = std::optional<std::pair<String, ColumnFilterPtr>>;
class Int64RangeFilter : public ColumnFilter
{
public:
    static OptionalFilter create(const ActionsDAG::Node & node);
    explicit Int64RangeFilter(const Int64 min_, const Int64 max_, bool null_allowed_)
        : ColumnFilter(Int64Range, null_allowed_)
        , max(max_)
        , min(min_)
        , lower32(static_cast<Int32>(std::max<Int64>(min, std::numeric_limits<int32_t>::min())))
        , upper32(static_cast<Int32>(std::min<Int64>(max, std::numeric_limits<int32_t>::max())))
        , lower16(static_cast<Int16>(std::max<Int64>(min, std::numeric_limits<int16_t>::min())))
        , upper16(static_cast<Int16>(std::min<Int64>(max, std::numeric_limits<int16_t>::max())))
        , is_single_value(max == min)
    {
    }
    ~Int64RangeFilter() override = default;
    bool testInt64(Int64 int64) const override { return int64 >= min && int64 <= max; }
    bool testInt32(Int32 int32) const override { return int32 >= min && int32 <= max; }
    bool testInt16(Int16 int16) const override { return int16 >= min && int16 <= max; }
    bool testInt64Range(Int64 lower, Int64 upper) const override { return min >= lower && max <= upper; }
    void testInt64Values(RowSet & row_set, size_t offset, size_t len, const Int64 * data) const override;
    void testInt32Values(RowSet & row_set, size_t offset, size_t len, const Int32 * data) const override;
    void testInt16Values(RowSet & row_set, size_t offset, size_t len, const Int16 * data) const override;
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;
    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override
    {
        return std::make_shared<Int64RangeFilter>(min, max, null_allowed_.value_or(null_allowed));
    }
    String toString() const override
    {
        return fmt::format("Int64RangeFilter: [{}, {}] {}", min, max, null_allowed ? "with nulls" : "no nulls");
    }

private:
    template<class T>
    void testIntValues(RowSet & row_set, size_t offset, size_t len, const T * data) const;

    const Int64 max;
    const Int64 min;
    const int32_t lower32;
    const int32_t upper32;
    const int16_t lower16;
    const int16_t upper16;
    bool is_single_value [[maybe_unused]];
};
class ByteValuesFilter : public ColumnFilter
{
public:
    static OptionalFilter create(const ActionsDAG::Node & node);
    ByteValuesFilter(const std::vector<String> & values_, bool null_allowed_)
        : ColumnFilter(ByteValues, null_allowed_), values(values_.begin(), values_.end())
    {
        std::ranges::for_each(values_, [&](const String & value) { lengths.insert(value.size()); });
        lower = *std::min_element(values_.begin(), values_.end());
        upper = *std::max_element(values_.begin(), values_.end());
    }

    ByteValuesFilter(const ByteValuesFilter & other, bool nullAllowed)
        : ColumnFilter(ColumnFilterKind::ByteValues, nullAllowed)
        , lower(other.lower)
        , upper(other.upper)
        , values(other.values)
        , lengths(other.lengths)
    {
    }

    bool testString(const String & value) const override { return lengths.contains(value.size()) && values.contains(value); }
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;
    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override
    {
        return std::make_shared<ByteValuesFilter>(*this, null_allowed_.value_or(null_allowed));
    }

    String toString() const override { return "ByteValuesFilter(" + lower + ", " + upper + ")"; }

private:
    String lower;
    String upper;
    std::unordered_set<String> values;
    std::unordered_set<size_t> lengths;
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
        if (lower_unbounded && upper_unbounded)
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
    static OptionalFilter create(const ActionsDAG::Node & node)
    {
        if (!isCompareColumnWithConst(node))
            return std::nullopt;
        const auto * input_node = getInputNode(node);
        auto name = input_node->result_name;
        if (!isFloat(input_node->result_type))
            return std::nullopt;
        auto constant_nodes = getConstantNode(node);
        auto func_name = node.function_base->getName();
        T value;
        if constexpr (std::is_same_v<T, Float32>)
            value = constant_nodes.front()->column->getFloat32(0);
        else
            value = constant_nodes.front()->column->getFloat64(0);
        ColumnFilterPtr filter = nullptr;
        if (func_name == "less")
        {
            filter = std::make_shared<FloatRangeFilter<T>>(-std::numeric_limits<T>::infinity(), true, false, value, false, false, false);
        }
        else if (func_name == "greater")
        {
            filter = std::make_shared<FloatRangeFilter<T>>(value, false, false, std::numeric_limits<T>::infinity(), true, false, false);
        }
        else if (func_name == "lessOrEquals")
        {
            filter = std::make_shared<FloatRangeFilter<T>>(-std::numeric_limits<T>::infinity(), true, true, value, false, false, false);
        }
        else if (func_name == "greaterOrEquals")
        {
            filter = std::make_shared<FloatRangeFilter<T>>(value, false, false, std::numeric_limits<T>::infinity(), true, true, false);
        }
        if (filter)
            return std::make_optional(std::make_pair(name, filter));
        else
            return std::nullopt;
    }
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

    FloatRangeFilter(const FloatRangeFilter & other, bool null_allowed_)
        : AbstractRange(
              other.lower_unbounded,
              other.lower_exclusive,
              other.upper_unbounded,
              other.upper_exclusive,
              null_allowed_,
              (std::is_same_v<T, double>) ? ColumnFilterKind::DoubleRange : ColumnFilterKind::FloatRange)
        , min(other.min)
        , max(other.max)
    {
        chassert(lower_unbounded || !std::isnan(min));
        chassert(upper_unbounded || !std::isnan(max));
    }

    bool testFloat32(Float32 value) const override { return testFloatingPoint(value); }
    bool testFloat64(Float64 value) const override { return testFloatingPoint(static_cast<T>(value)); }
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;
    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override
    {
        return std::make_shared<FloatRangeFilter>(*this, null_allowed_.value_or(null_allowed));
    }
    String toString() const override
    {
        return fmt::format(
            "FloatRangeFilter: {}{}, {}{} {}",
            (lower_exclusive || lower_unbounded) ? "(" : "[",
            lower_unbounded ? "-inf" : std::to_string(min),
            upper_unbounded ? "nan" : std::to_string(max),
            (upper_exclusive && !upper_unbounded) ? ")" : "]",
            null_allowed ? "with nulls" : "no nulls");
    }

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

using Float32RangeFilter = FloatRangeFilter<Float32>;
using Float64RangeFilter = FloatRangeFilter<Float64>;

OptionalFilter createFloatRangeFilter(const ActionsDAG::Node & node);
}
