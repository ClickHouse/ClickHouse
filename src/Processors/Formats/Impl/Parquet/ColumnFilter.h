#pragma once
#include <iostream>
#include <unordered_set>
#include <Columns/ColumnsCommon.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <base/types.h>
#include <boost/dynamic_bitset.hpp>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}
class RowSet
{
public:
    explicit RowSet(size_t max_rows_)
        : max_rows(max_rows_)
    {
        mask.resize(max_rows, true);
        std::fill(mask.begin(), mask.end(), true);
    }

    explicit RowSet(PaddedPODArray<bool> & mask_)
        : max_rows(mask_.size())
    {
        mask.swap(mask_);
    }

    inline void set(size_t i, bool value) { mask[offset + i] = value; }
    inline bool get(size_t i) const
    {
        auto position = offset + i;
        if (position >= max_rows)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "RowSet index out of bound: {} >= {}", i, max_rows);
        return mask[position];
    }
    inline size_t totalRows() const { return max_rows; }
    inline void setOffset(size_t offset_) { offset = offset_; }
    inline void addOffset(size_t offset_) { offset += offset_; }
    inline size_t getOffset() const { return offset; }
    bool none() const;
    bool all() const;
    bool any() const;
    void setAllTrue() { std::fill(mask.begin(), mask.end(), true); }
    void setAllFalse() { std::fill(mask.begin(), mask.end(), false); }
    size_t count() const { return countBytesInFilter(reinterpret_cast<const UInt8 *>(mask.data()), 0, mask.size()); }

    const bool * activeAddress() const { return mask.data() + offset; }
    bool * activeAddress() { return mask.data() + offset; }

private:
    size_t max_rows = 0;
    size_t offset = 0;
    PaddedPODArray<bool> mask;
};
using OptionalRowSet = std::optional<RowSet>;

class ColumnFilter;
using ColumnFilterPtr = std::shared_ptr<ColumnFilter>;

enum ColumnFilterKind
{
    Unknown,
    AlwaysTrue,
    AlwaysFalse,
    IsNull,
    IsNotNull,
    BoolValue,
    BigIntRange,
    BigIntValuesUsingHashTable,
    BigIntValuesUsingBitmask,
    NegatedBigIntRange,
    NegatedBigIntValuesUsingHashTable,
    NegatedBigIntValuesUsingBitmask,
    FloatRange,
    DoubleRange,
    BytesRange,
    NegatedBytesRange,
    BytesValues,
    NegatedBytesValues,
    BigIntMultiRange
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

class ExpressionFilter
{
public:
    explicit ExpressionFilter(ActionsDAG && dag_);
    NameSet getInputs();

    IColumn::Filter execute(const ColumnsWithTypeAndName & columns) const;

private:
    ExpressionActionsPtr actions;
    String filter_name;
};
/// The purpose of ColumnFilter is to reduce unnecessary memory copies by calculating expressions in existing memory (decompression buffer).
/// The following code mainly refers to https://github.com/facebookincubator/velox/blob/main/velox/type/Filter.h
class ColumnFilter
{
protected:
    ColumnFilter(ColumnFilterKind kind, bool null_allowed_)
        : kind_(kind)
        , null_allowed(null_allowed_)
    {
    }

public:
    virtual ~ColumnFilter() = default;
    virtual ColumnFilterKind kind() const { return kind_; }
    virtual ColumnPtr testByExpression(ColumnPtr)
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testByExpression not supported", toString());
    }
    virtual bool testNull() const { return null_allowed; }
    virtual bool testNotNull() const { throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testNotNull not supported", toString()); }
    virtual bool testInt64(Int64) const { throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testInt64 not supported", toString()); }
    virtual bool testInt32(Int32) const { throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testInt32 not supported", toString()); }
    virtual bool testInt16(Int16) const { throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testInt16 not supported", toString()); }
    virtual bool testFloat32(Float32) const { throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testFloat32 not supported", toString()); }
    virtual bool testFloat64(Float64) const { throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testFloat64 not supported", toString()); }
    virtual bool testBool(bool) const { throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testBool not supported", toString()); }
    virtual bool testString(const std::string_view & /*value*/) const
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testString not supported", toString());
    }

    virtual bool testInt64Range(Int64 /*min*/, Int64 /*max*/, bool /*has_null*/) const
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testInt64Range not supported", toString());
    }

    virtual bool testFloat32Range(Float32 /*min*/, Float32 /*max*/, bool /*has_null*/) const
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testFloat32Range not supported", toString());
    }

    virtual bool testFloat64Range(Float64 /*min*/, Float64 /*max*/, bool /*has_null*/) const
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testFloat64Range not supported", toString());
    }

    virtual bool testStringRange(std::optional<std::string_view> /*min*/, std::optional<std::string_view> /*max*/, bool /*has_null*/) const
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} testStringRange not supported", toString());
    }

    virtual void testInt64Values(RowSet & row_set, size_t len, const Int64 * data) const
    {
        for (size_t i = 0; i < len; ++i)
        {
            row_set.set(i, testInt64(data[i]));
        }
    }

    virtual void testInt32Values(RowSet & row_set, size_t len, const Int32 * data) const
    {
        for (size_t i = 0; i < len; ++i)
        {
            row_set.set(i, testInt32(data[i]));
        }
    }

    virtual void testInt16Values(RowSet & row_set, size_t len, const Int16 * data) const
    {
        for (size_t i = 0; i < len; ++i)
        {
            row_set.set(i, testInt16(data[i]));
        }
    }

    virtual void testFloat32Values(RowSet & row_set, size_t len, const Float32 * data) const
    {
        for (size_t i = 0; i < len; ++i)
        {
            row_set.set(i, testFloat32(data[i]));
        }
    }

    virtual void testFloat64Values(RowSet & row_set, size_t len, const Float64 * data) const
    {
        for (size_t i = 0; i < len; ++i)
        {
            row_set.set(i, testFloat64(data[i]));
        }
    }


    virtual ColumnFilterPtr merge(const ColumnFilter * /*filter*/) const
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{} merge not supported", toString());
    }

    virtual ColumnFilterPtr clone(std::optional<bool> null_allowed_) const = 0;

    virtual String toString() const
    {
        return fmt::format("Filter({}, {})", magic_enum::enum_name(kind()), null_allowed ? "null allowed" : "null not allowed");
    }

protected:
    ColumnFilterKind kind_;
    bool null_allowed;
};
using OptionalFilter = std::optional<std::pair<String, ColumnFilterPtr>>;

class IsNullFilter : public ColumnFilter
{
public:
    static OptionalFilter create(const ActionsDAG::Node & node);
    IsNullFilter()
        : ColumnFilter(IsNull, true)
    {
    }
    ~IsNullFilter() override = default;
    bool testNotNull() const override { return false; }
    bool testNull() const override { return true; }
    bool testInt64(Int64) const override { return false; }
    bool testInt32(Int32) const override { return false; }
    bool testInt16(Int16) const override { return false; }
    bool testFloat32(Float32) const override { return false; }
    bool testFloat64(Float64) const override { return false; }
    bool testBool(bool) const override { return true; }
    bool testInt64Range(Int64, Int64, bool) const override { return false; }
    bool testFloat32Range(Float32, Float32, bool) const override { return false; }
    bool testFloat64Range(Float64, Float64, bool) const override { return false; }
    bool testString(const std::string_view &) const override { return false; }
    ColumnFilterPtr clone(std::optional<bool>) const override { return std::make_shared<IsNullFilter>(); }
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;
};

class IsNotNullFilter : public ColumnFilter
{
public:
    static OptionalFilter create(const ActionsDAG::Node & node);
    IsNotNullFilter()
        : ColumnFilter(IsNotNull, false)
    {
    }
    ~IsNotNullFilter() override = default;
    bool testNotNull() const override { return true; }
    bool testNull() const override { return false; }
    bool testInt64(Int64) const override { return true; }
    bool testInt32(Int32) const override { return true; }
    bool testInt16(Int16) const override { return true; }
    bool testFloat32(Float32) const override { return true; }
    bool testFloat64(Float64) const override { return true; }
    bool testBool(bool) const override { return true; }
    bool testInt64Range(Int64, Int64, bool) const override { return true; }
    bool testFloat32Range(Float32, Float32, bool) const override { return true; }
    bool testFloat64Range(Float64, Float64, bool) const override { return true; }
    bool testString(const std::string_view & /*value*/) const override { return true; }
    ColumnFilterPtr clone(std::optional<bool>) const override { return std::make_shared<IsNotNullFilter>(); }
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;
};

class AlwaysTrueFilter : public ColumnFilter
{
public:
    AlwaysTrueFilter()
        : ColumnFilter(AlwaysTrue, true)
    {
    }
    ~AlwaysTrueFilter() override = default;
    bool testNull() const override { return true; }
    bool testNotNull() const override { return true; }
    bool testInt64(Int64) const override { return true; }
    bool testFloat32(Float32) const override { return true; }
    bool testFloat64(Float64) const override { return true; }
    bool testBool(bool) const override { return true; }
    bool testInt64Range(Int64, Int64, bool) const override { return true; }
    bool testFloat32Range(Float32, Float32, bool) const override { return true; }
    bool testFloat64Range(Float64, Float64, bool) const override { return true; }
    void testInt64Values(RowSet & set, size_t, const Int64 *) const override { set.setAllTrue(); }
    void testInt32Values(RowSet & set, size_t, const Int32 *) const override { set.setAllTrue(); }
    void testInt16Values(RowSet & set, size_t, const Int16 *) const override { set.setAllTrue(); }
    bool testString(const std::string_view & /*value*/) const override { return true; }
    ColumnFilterPtr merge(const ColumnFilter * /*filter*/) const override { return std::make_shared<AlwaysTrueFilter>(); }
    ColumnFilterPtr clone(std::optional<bool>) const override { return std::make_shared<AlwaysTrueFilter>(); }
};

class AlwaysFalseFilter : public ColumnFilter
{
public:
    AlwaysFalseFilter()
        : ColumnFilter(AlwaysFalse, false)
    {
    }
    ~AlwaysFalseFilter() override = default;
    bool testNull() const override { return false; }
    bool testNotNull() const override { return false; }
    bool testInt64(Int64) const override { return false; }
    bool testFloat32(Float32) const override { return false; }
    bool testFloat64(Float64) const override { return false; }
    bool testBool(bool) const override { return true; }
    bool testInt64Range(Int64, Int64, bool) const override { return false; }
    bool testFloat32Range(Float32, Float32, bool) const override { return false; }
    bool testFloat64Range(Float64, Float64, bool) const override { return false; }
    void testInt64Values(RowSet & set, size_t, const Int64 *) const override { set.setAllFalse(); }
    void testInt32Values(RowSet & set, size_t, const Int32 *) const override { set.setAllFalse(); }
    void testInt16Values(RowSet & set, size_t, const Int16 *) const override { set.setAllFalse(); }
    bool testString(const std::string_view & /*value*/) const override { return false; }
    ColumnFilterPtr merge(const ColumnFilter * /*filter*/) const override { return std::make_shared<AlwaysFalseFilter>(); }
    ColumnFilterPtr clone(std::optional<bool>) const override { return std::make_shared<AlwaysFalseFilter>(); }
};

class BoolValueFilter : public ColumnFilter
{
public:
    explicit BoolValueFilter(bool value_, bool null_allowed_)
        : ColumnFilter(BoolValue, null_allowed_)
        , value(value_)
    {
    }
    bool testBool(bool value_) const override { return value_ == value; }
    bool testInt64(Int64 value_) const override { return value == (value_ != 0); }
    bool testInt64Range(Int64 min, Int64 max, bool has_null) const override
    {
        if (has_null && null_allowed)
            return true;
        if (value)
            return min != 0 || max != 0;
        else
            return min <= 0 && max >= 0;
    }
    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override
    {
        if (null_allowed_)
            return std::make_shared<BoolValueFilter>(this->value, null_allowed_.value());
        else
            return std::make_shared<BoolValueFilter>(value, null_allowed);
    }
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;

private:
    const bool value;
};


class BigIntRangeFilter : public ColumnFilter
{
    friend class NegatedBigIntRangeFilter;

public:
    explicit BigIntRangeFilter(const Int64 lower_, const Int64 max_, bool null_allowed_)
        : ColumnFilter(BigIntRange, null_allowed_)
        , upper(max_)
        , lower(lower_)
        , lower32(static_cast<Int32>(std::max<Int64>(lower, std::numeric_limits<int32_t>::min())))
        , upper32(static_cast<Int32>(std::min<Int64>(upper, std::numeric_limits<int32_t>::max())))
        , lower16(static_cast<Int16>(std::max<Int64>(lower, std::numeric_limits<int16_t>::min())))
        , upper16(static_cast<Int16>(std::min<Int64>(upper, std::numeric_limits<int16_t>::max())))
        , is_single_value(upper == lower)
    {
    }
    ~BigIntRangeFilter() override = default;
    bool testInt64(Int64 int64) const override { return int64 >= lower && int64 <= upper; }
    bool testInt32(Int32 int32) const override { return int32 >= lower && int32 <= upper; }
    bool testInt16(Int16 int16) const override { return int16 >= lower && int16 <= upper; }
    bool testInt64Range(Int64 min, Int64 max, bool has_null) const override
    {
        if (has_null && null_allowed)
            return true;
        return !(min > upper || max < lower);
    }
    void testInt64Values(RowSet & row_set, size_t len, const Int64 * data) const override;
    void testInt32Values(RowSet & row_set, size_t len, const Int32 * data) const override;
    void testInt16Values(RowSet & row_set, size_t len, const Int16 * data) const override;
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;
    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override
    {
        return std::make_shared<BigIntRangeFilter>(lower, upper, null_allowed_.value_or(null_allowed));
    }

    String toString() const override
    {
        return fmt::format("BigIntRangeFilter: [{}, {}] {}", lower, upper, null_allowed ? "with nulls" : "no nulls");
    }

    Int64 getUpper() const { return upper; }
    Int64 getLower() const { return lower; }
    bool isSingleValue() const { return is_single_value; }

private:
    template <class T, bool negated = false>
    void testIntValues(RowSet & row_set, size_t len, const T * data) const;

    const Int64 upper;
    const Int64 lower;
    const int32_t lower32;
    const int32_t upper32;
    const int16_t lower16;
    const int16_t upper16;
    bool is_single_value [[maybe_unused]];
};

class NegatedBigIntRangeFilter : public ColumnFilter
{
public:
    NegatedBigIntRangeFilter(int64_t lower_, int64_t upper_, bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::NegatedBigIntRange, null_allowed_)
        , non_negated(std::make_unique<BigIntRangeFilter>(lower_, upper_, !null_allowed_))
    {
    }

    ~NegatedBigIntRangeFilter() override;
    bool testInt64(Int64 int64) const override { return !non_negated->testInt64(int64); }
    bool testInt32(Int32 int32) const override { return !non_negated->testInt32(int32); }
    bool testInt16(Int16 int16) const override { return !non_negated->testInt16(int16); }
    bool testInt64Range(Int64 min, Int64 max, bool has_null) const override
    {
        if (has_null && null_allowed)
        {
            return true;
        }
        return !(non_negated->getLower() <= min && max <= non_negated->getUpper());
    }
    void testInt64Values(RowSet & row_set, size_t len, const Int64 * data) const override
    {
        non_negated->testIntValues<Int64, true>(row_set, len, data);
    }
    void testInt32Values(RowSet & row_set, size_t len, const Int32 * data) const override
    {
        non_negated->testIntValues<Int32, true>(row_set, len, data);
    }
    void testInt16Values(RowSet & row_set, size_t len, const Int16 * data) const override
    {
        non_negated->testIntValues<Int16, true>(row_set, len, data);
    }

    ColumnFilterPtr merge(const ColumnFilter * filter) const override;
    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override;
    String toString() const override
    {
        return fmt::format(
            "NegatedBigIntRangeFilter: [{}, {}] {}", non_negated->lower, non_negated->upper, null_allowed ? "with nulls" : "no nulls");
    }
    Int64 getUpper() const { return non_negated->getLower(); }
    Int64 getLower() const { return non_negated->getUpper(); }

private:
    std::unique_ptr<BigIntRangeFilter> non_negated;
};

class BigIntValuesUsingHashTableFilter final : public ColumnFilter
{
public:
    BigIntValuesUsingHashTableFilter(int64_t min, int64_t max, const std::vector<int64_t> & values, bool null_allowed_);

    BigIntValuesUsingHashTableFilter(const BigIntValuesUsingHashTableFilter & other, bool null_allowed_)
        : ColumnFilter(other.kind(), null_allowed_)
        , min(other.min)
        , max(other.max)
        , hashTable(other.hashTable)
        , containsEmptyMarker(other.containsEmptyMarker)
        , values(other.values)
        , sizeMask(other.sizeMask)
    {
    }

    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override
    {
        if (null_allowed_)
        {
            return std::make_shared<BigIntValuesUsingHashTableFilter>(*this, null_allowed_.value());
        }
        else
        {
            return std::make_shared<BigIntValuesUsingHashTableFilter>(*this, null_allowed);
        }
    }

    bool testInt64(Int64 value) const override;
    bool testInt32(Int32 value) const override { return testInt64(value); }
    bool testInt16(Int16 value) const override { return testInt64(value); }
    bool testInt64Range(Int64 min_, Int64 max_, bool has_null) const override;
    void testInt64Values(RowSet & row_set, size_t len, const Int64 * data) const override
    {
        // TODO support simd
        ColumnFilter::testInt64Values(row_set, len, data);
    }
    void testInt32Values(RowSet & row_set, size_t len, const Int32 * data) const override
    {
        ColumnFilter::testInt32Values(row_set, len, data);
    }
    void testInt16Values(RowSet & row_set, size_t len, const Int16 * data) const override
    {
        ColumnFilter::testInt16Values(row_set, len, data);
    }

    Int64 getMin() const { return min; }

    Int64 getMax() const { return max; }

    std::vector<int64_t> getValues() const { return values; }

private:
    static constexpr int64_t kEmptyMarker = 0xdeadbeefbadefeedL;
    // from Murmur hash
    static constexpr uint64_t M = 0xc6a4a7935bd1e995L;

    const int64_t min;
    const int64_t max;
    std::vector<int64_t> hashTable;
    bool containsEmptyMarker = false;
    std::vector<int64_t> values;
    int32_t sizeMask;
};

class BigIntValuesUsingBitmaskFilter : public ColumnFilter
{
public:
    BigIntValuesUsingBitmaskFilter(Int64 min_, Int64 max_, const std::vector<Int64> & values_, bool null_allowed_);

    BigIntValuesUsingBitmaskFilter(const BigIntValuesUsingBitmaskFilter & other, bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::BigIntValuesUsingBitmask, null_allowed_)
        , bitmask(other.bitmask)
        , min(other.min)
        , max(other.max)
    {
    }
    ~BigIntValuesUsingBitmaskFilter() override = default;

    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const final
    {
        if (null_allowed_)
            return std::make_shared<BigIntValuesUsingBitmaskFilter>(*this, null_allowed_.value());
        else
            return std::make_shared<BigIntValuesUsingBitmaskFilter>(*this, null_allowed);
    }

    std::vector<Int64> values() const;

    bool testInt64(Int64 value) const final;

    bool testInt64Range(Int64 min_, Int64 max_, bool has_null) const final;

    ColumnFilterPtr merge(const ColumnFilter * other) const override;


private:
    ColumnFilterPtr merge(int64_t min, int64_t max, const ColumnFilter * other) const;
    std::vector<bool> bitmask;
    const int64_t min;
    const int64_t max;
};

class NegatedBigIntValuesUsingHashTableFilter : public ColumnFilter
{
public:
    NegatedBigIntValuesUsingHashTableFilter(int64_t min_, int64_t max_, const std::vector<int64_t> & values_, bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::NegatedBigIntValuesUsingHashTable, null_allowed_)
        , non_negated(std::make_shared<BigIntValuesUsingHashTableFilter>(min_, max_, values_, null_allowed_))
    {
    }

    NegatedBigIntValuesUsingHashTableFilter(const NegatedBigIntValuesUsingHashTableFilter & other, bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::NegatedBigIntValuesUsingHashTable, null_allowed_)
        , non_negated(std::make_shared<BigIntValuesUsingHashTableFilter>(*other.non_negated, null_allowed))
    {
    }

    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const final
    {
        return std::make_shared<NegatedBigIntValuesUsingHashTableFilter>(*this, null_allowed_.value_or(null_allowed));
    }

    bool testInt64(int64_t value) const final { return !non_negated->testInt64(value); }

    /// TODO support batch test

    bool testInt64Range(int64_t min, int64_t max, bool has_null) const final;

    ColumnFilterPtr merge(const ColumnFilter * other) const override;

    int64_t getMin() const { return non_negated->getMin(); }

    int64_t getMax() const { return non_negated->getMax(); }

    std::vector<int64_t> getValues() const { return non_negated->getValues(); }

    std::string toString() const final
    {
        return fmt::format(
            "NegatedBigintValuesUsingHashTable: [{}, {}] {}",
            non_negated->getMin(),
            non_negated->getMax(),
            null_allowed ? "with nulls" : "no nulls");
    }

private:
    std::shared_ptr<BigIntValuesUsingHashTableFilter> non_negated;
};

class NegatedBigIntValuesUsingBitmaskFilter : public ColumnFilter
{
public:
    NegatedBigIntValuesUsingBitmaskFilter(int64_t min_, int64_t max_, const std::vector<int64_t> & values_, bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::NegatedBigIntValuesUsingBitmask, null_allowed_)
        , min(min_)
        , max(max_)
    {
        if (min > max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "min must be no greater than max");
        non_negated = std::make_shared<BigIntValuesUsingBitmaskFilter>(min_, max_, values_, null_allowed_);
    }

    NegatedBigIntValuesUsingBitmaskFilter(const NegatedBigIntValuesUsingBitmaskFilter & other, bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::NegatedBigIntValuesUsingBitmask, null_allowed_)
        , min(other.min)
        , max(other.max)
        , non_negated(std::make_shared<BigIntValuesUsingBitmaskFilter>(*other.non_negated, null_allowed_))
    {
    }

    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const final
    {
        return std::make_unique<NegatedBigIntValuesUsingBitmaskFilter>(*this, null_allowed_.value_or(null_allowed));
    }

    std::vector<int64_t> getValues() const { return non_negated->values(); }

    bool testInt64(int64_t value) const final { return !non_negated->testInt64(value); }

    bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final;

    ColumnFilterPtr merge(const ColumnFilter * other) const override;

private:
    Int64 min;
    Int64 max;
    std::shared_ptr<BigIntValuesUsingBitmaskFilter> non_negated;
};

class BytesValuesFilter : public ColumnFilter
{
    friend class NegatedBytesValuesFilter;

public:
    BytesValuesFilter(const std::vector<String> & values_, bool null_allowed_)
        : ColumnFilter(BytesValues, null_allowed_)
        , values_storage(values_)
        , values(values_storage.begin(), values_storage.end())
    {
        std::ranges::for_each(values_, [&](const String & value) { lengths.insert(value.size()); });
        lower = *std::min_element(values_.begin(), values_.end());
        upper = *std::max_element(values_.begin(), values_.end());
    }

    BytesValuesFilter(const BytesValuesFilter & other, bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::BytesValues, null_allowed_)
        , lower(other.lower)
        , upper(other.upper)
        , values_storage(other.values_storage)
        , values(values_storage.begin(), values_storage.end())
        , lengths(other.lengths)
    {
    }
    ~BytesValuesFilter() override;
    bool testString(const std::string_view & value) const override { return lengths.contains(value.size()) && values.contains(value); }
    bool testStringRange(std::optional<std::string_view> anOptional, std::optional<std::string_view> anOptional1, bool b) const override;
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;
    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override
    {
        return std::make_shared<BytesValuesFilter>(*this, null_allowed_.value_or(null_allowed));
    }

    String toString() const override { return "ByteValuesFilter(" + lower + ", " + upper + ")"; }

    const std::unordered_set<std::string_view> & getValues() const { return values; }

private:
    String lower;
    String upper;
    std::vector<String> values_storage;
    std::unordered_set<std::string_view> values;
    std::unordered_set<size_t> lengths;
};

class NegatedBytesValuesFilter : public ColumnFilter
{
public:
    NegatedBytesValuesFilter(const std::vector<String> & values_, bool null_allowed_)
        : ColumnFilter(NegatedBytesValues, null_allowed_)
        , non_negated(std::make_unique<BytesValuesFilter>(values_, !null_allowed_))
    {
    }
    NegatedBytesValuesFilter(const NegatedBytesValuesFilter & other, bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::NegatedBytesValues, null_allowed_)
        , non_negated(std::make_unique<BytesValuesFilter>(*other.non_negated, other.non_negated->null_allowed))
    {
    }
    ~NegatedBytesValuesFilter() override;
    bool testString(const std::string_view & value) const override { return !non_negated->testString(value); }
    ColumnFilterPtr merge(const ColumnFilter * filter) const override;
    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override;

private:
    std::unique_ptr<BytesValuesFilter> non_negated;
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
concept is_float = std::is_same_v<T, double> || std::is_same_v<T, float>;

template <is_float T>
class FloatRangeFilter : public AbstractRange
{
public:
    FloatRangeFilter(
        const T min_, bool lowerUnbounded, bool lowerExclusive, const T max_, bool upperUnbounded, bool upperExclusive, bool null_allowed_)
        : AbstractRange(
              lowerUnbounded,
              lowerExclusive,
              upperUnbounded,
              upperExclusive,
              null_allowed_,
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

    ~FloatRangeFilter() override = default;
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

class BytesRangeFilter : public AbstractRange
{
public:
    BytesRangeFilter(
        const std::string & lower_,
        bool lower_unbounded_,
        bool lower_exclusive_,
        const std::string & upper_,
        bool upper_unbounded_,
        bool upper_exclusive_,
        bool null_allowed_)
        : AbstractRange(lower_unbounded_, lower_exclusive_, upper_unbounded_, upper_exclusive_, null_allowed_, ColumnFilterKind::BytesRange)
        , lower(lower_)
        , upper(upper_)
        , single_value(!lower_exclusive_ && !upper_exclusive_ && !lower_unbounded_ && !upper_unbounded_ && lower_ == upper_)
    {
        // Always-true filters should be specified using AlwaysTrue.
        if (!(!lower_unbounded_ || !upper_unbounded_))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A range filter must have a lower or upper bound");
        }
    }

    BytesRangeFilter(const BytesRangeFilter & other, bool null_allowed_)
        : AbstractRange(
              other.lower_unbounded,
              other.lower_exclusive,
              other.upper_unbounded,
              other.upper_exclusive,
              null_allowed_,
              ColumnFilterKind::BytesRange)
        , lower(other.lower)
        , upper(other.upper)
        , single_value(other.single_value)
    {
    }

    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const final
    {
        if (null_allowed_)
        {
            return std::make_shared<BytesRangeFilter>(*this, null_allowed_.value());
        }
        else
        {
            return std::make_shared<BytesRangeFilter>(*this, null_allowed);
        }
    }

    std::string toString() const final
    {
        return fmt::format(
            "BytesRange: {}{}, {}{} {}",
            (lower_unbounded || lower_exclusive) ? "(" : "[",
            lower_unbounded ? "..." : lower,
            upper_unbounded ? "..." : upper,
            (upper_unbounded || upper_exclusive) ? ")" : "]",
            null_allowed ? "with nulls" : "no nulls");
    }

    bool testString(const std::string_view & value) const override;

    bool testStringRange(std::optional<std::string_view> min, std::optional<std::string_view> max, bool has_null) const override;

    ColumnFilterPtr merge(const ColumnFilter * other) const override;

    bool isSingleValue() const { return single_value; }

    bool isUpperUnbounded() const { return upper_unbounded; }

    bool isLowerUnbounded() const { return lower_unbounded; }

    const std::string & getLower() const { return lower; }

    const std::string & getUpper() const { return upper; }

private:
    const std::string lower;
    const std::string upper;
    const bool single_value;
};

class NegatedBytesRangeFilter : public ColumnFilter
{
public:
    NegatedBytesRangeFilter(
        const std::string & lower_,
        bool lower_unbounded_,
        bool lower_exclusive_,
        const std::string & upper_,
        bool upper_unbounded_,
        bool upper_exclusive_,
        bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::NegatedBytesRange, null_allowed_)
        , non_negated(std::make_shared<BytesRangeFilter>(
              lower_, lower_unbounded_, lower_exclusive_, upper_, upper_unbounded_, upper_exclusive_, !null_allowed_))
    {
    }

    NegatedBytesRangeFilter(const NegatedBytesRangeFilter & other, bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::NegatedBytesRange, null_allowed_)
        , non_negated(std::make_shared<BytesRangeFilter>(*other.non_negated, null_allowed_))
    {
    }

    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override
    {
        return std::make_shared<NegatedBytesRangeFilter>(*this, null_allowed_.value_or(null_allowed));
    }

    std::string toString() const final { return "Negated" + non_negated->toString(); }

    bool testString(const std::string_view & value) const override { return !non_negated->testString(value); }

    bool testStringRange(std::optional<std::string_view> min, std::optional<std::string_view> max, bool has_null) const override
    {
        return !non_negated->testStringRange(min, max, has_null);
    }

    ColumnFilterPtr merge(const ColumnFilter * other) const override;

    bool isSingleValue() const { return non_negated->isSingleValue(); }

    bool isUpperUnbounded() const { return non_negated->isUpperUnbounded(); }

    bool isLowerUnbounded() const { return non_negated->isLowerUnbounded(); }

    const std::string & getLower() const { return non_negated->getLower(); }

    const std::string & getUpper() const { return non_negated->getUpper(); }

private:
    std::shared_ptr<BytesRangeFilter> non_negated;
};

class BigIntMultiRangeFilter : public ColumnFilter
{
public:
    BigIntMultiRangeFilter(std::vector<std::shared_ptr<BigIntRangeFilter>> ranges, bool null_allowed_);

    BigIntMultiRangeFilter(const BigIntMultiRangeFilter & other, bool null_allowed_)
        : ColumnFilter(ColumnFilterKind::BigIntMultiRange, null_allowed_)
        , ranges(other.ranges)
        , lower_bounds(other.lower_bounds)
    {
    }

    ColumnFilterPtr clone(std::optional<bool> null_allowed_) const override;

    bool testInt64(int64_t value) const override;

    bool testInt64Range(int64_t min, int64_t max, bool has_null) const override;

    ColumnFilterPtr merge(const ColumnFilter * other) const override;

    const std::vector<std::shared_ptr<BigIntRangeFilter>> & getRanges() const { return ranges; }

    std::string toString() const override
    {
        WriteBufferFromOwnString out;
        writeString("BigintMultiRange: [", out);
        for (const auto & range : ranges)
        {
            writeString(" " + range->toString(), out);
        }
        writeString(" ]", out);
        writeString(null_allowed ? "with nulls" : "no nulls", out);
        return out.str();
    }

private:
    const std::vector<std::shared_ptr<BigIntRangeFilter>> ranges;
    std::vector<int64_t> lower_bounds;
};
}
