#pragma once

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>

#include <algorithm>
#include <cinttypes>
#include <cmath>
#include <vector>


namespace DB {

Float64 sampleLaplace();

template <typename Value, bool is_integer = std::numeric_limits<Value>::is_integer>
struct BinsHelper;

template <typename Value>
struct PrivateMaxAbs;

template <typename Value>
using SumAccumulatorType = std::conditional_t<IsDecimalNumber<Value>, Decimal128, NearestFieldType<Value>>;


template <typename Value>
struct AggregateFunctionDPBase {
    AggregateFunctionDPBase(const DataTypes & argument_types_, const Array & parameters_):
            scale(getDecimalScale(*argument_types_[0]))
            , eps(applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]))
    {}

public:
    UInt32 scale; // For Decimals
    Float64 eps; // DP param
};

template <typename Value>
struct ToArrayData
{
    static constexpr size_t bytes_in_arena = 64 - sizeof(PODArray<Value>);
    using Array = PODArrayWithStackMemory<Value, bytes_in_arena>;
    mutable Array array; /// mutable because array sorting is not considered a state change.

    void sort() const
    {
        /// Sorting an array will not be considered a violation of constancy.
        auto & mutable_array = const_cast<Array &>(array);
        std::sort(mutable_array.begin(), mutable_array.end());
    }
};


template <typename Value>
struct AllSavingAggregation : public IAggregateFunctionDataHelper<ToArrayData<Value>, AllSavingAggregation<Value>>,
                              public AggregateFunctionDPBase<Value>
{
    // To override:
    // String getName()
    // DataTypePtr getReturnType()
    // void insertResultInto(AggregateDataPtr place, IColumn & to)

    using ColVecType = std::conditional_t<IsDecimalNumber<Value>, ColumnDecimal<Value>, ColumnVector<Value>>;

    AllSavingAggregation(const DataTypes & argument_types_, const Array & params_) :
        IAggregateFunctionDataHelper<ToArrayData<Value>, AllSavingAggregation<Value>>(argument_types_, params_)
        , AggregateFunctionDPBase<Value>(argument_types_, params_)
    {}

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).array.push_back(static_cast<const ColVecType &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & other_array = this->data(rhs).array;
        this->data(place).array.insert(other_array.begin(), other_array.end());
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        auto & array = this->data(place).array;
        size_t size = array.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const  char *>(array.data()), size * sizeof(array[0]));
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override {
        auto &array = this->data(place).array;
        size_t size;
        readVarUInt(size, buf);
        array.resize(size);
        buf.read(reinterpret_cast<char *>(array.data()), size * sizeof(array[0]));
    }
};

Int128 abs(const Int128 & val) {
    if (val >= 0)
        return val;
    else
        return -val;
}

struct AggregateFunctionCountDPData
{
    UInt64 count = 0;
};

struct AggregateFunctionCountDP : public IAggregateFunctionDataHelper<AggregateFunctionCountDPData, AggregateFunctionCountDP>
{
    AggregateFunctionCountDP(const DataTypes & argument_types_, const Array & parameters_) :
        IAggregateFunctionDataHelper(argument_types_, parameters_),
        eps(applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]))
    {}

    String getName() const override { return "anon_count"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn **, size_t, Arena *) const override
    {
        ++data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count
        + static_cast<UInt64>(round(sampleLaplace() / eps)));
    }

private:
    Float64 eps; // DP parameter
};

template <typename Value>
struct AggregateFunctionDPBoundedSumData {
    SumAccumulatorType<Value> sum = 0;
};

template <typename Value>
struct AggregateFunctionSumDPBounded :
    public IAggregateFunctionDataHelper<AggregateFunctionDPBoundedSumData<Value>, AggregateFunctionSumDPBounded<Value>>,
    public AggregateFunctionDPBase<Value>
{
    using ResultType = SumAccumulatorType<Value>;
    using ResultDataType = std::conditional_t<IsDecimalNumber<Value>, DataTypeDecimal<ResultType>, DataTypeNumber<ResultType>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<Value>, ColumnDecimal<Value>, ColumnVector<Value>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<Value>, ColumnDecimal<Value>, ColumnVector<Value>>;


    AggregateFunctionSumDPBounded(const DataTypes & argument_types_, const Array & parameters_) :
        IAggregateFunctionDataHelper<AggregateFunctionDPBoundedSumData<Value>, AggregateFunctionSumDPBounded<Value>>(argument_types_, parameters_)
        , AggregateFunctionDPBase<Value>(argument_types_, parameters_)
        , max_abs(applyVisitor(FieldVisitorConvertToNumber<Value>(), parameters_[1]))
    {}

    String getName() const override
    {
        return "anon_sum";
    }

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimalNumber<Value>)
            return std::make_shared<ResultDataType>(ResultDataType::maxPrecision(), this->scale);
        else
            return std::make_shared<ResultDataType>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row, Arena *) const override
    {
        const auto & values = static_cast<const ColVecType &>(*columns[0]);
        if (std::numeric_limits<Value>::is_signed || IsDecimalNumber<Value>)
        {
            this->data(place).sum += std::clamp(values.getData()[row], Value(-max_abs), max_abs);
        }
        else
        {
            this->data(place).sum += std::clamp(values.getData()[row], Value(0), max_abs);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).sum += this->data(rhs).sum;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).sum, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).sum, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        auto & column = static_cast<ColVecResult &>(to);
        column.getData().push_back(this->data(place).sum
            + static_cast<UInt64>(round(sampleLaplace() / this->eps * static_cast<Float64>(this->max_abs))));
    }

private:
    Value max_abs; // Bounds
};


template <typename Value>
struct AggregateFunctionDPBoundedAvgData {
    SumAccumulatorType<Value> sum = 0;
    UInt64 count = 0;
};

template <typename Value>
struct AggregateFunctionAvgDPBounded :
        public IAggregateFunctionDataHelper<AggregateFunctionDPBoundedAvgData<Value>, AggregateFunctionAvgDPBounded<Value>>,
        public AggregateFunctionDPBase<Value>
{
    using ColVecType = std::conditional_t<IsDecimalNumber<Value>, ColumnDecimal<Value>, ColumnVector<Value>>;

    AggregateFunctionAvgDPBounded(const DataTypes & argument_types_, const Array & parameters_) :
            IAggregateFunctionDataHelper<AggregateFunctionDPBoundedAvgData<Value>, AggregateFunctionAvgDPBounded<Value>>(argument_types_, parameters_)
            , AggregateFunctionDPBase<Value>(argument_types_, parameters_)
            , high(applyVisitor(FieldVisitorConvertToNumber<Value>(), parameters_[1]))
    {
        if (parameters_.size() < 3)
        {
            if (IsDecimalNumber<Value> || std::numeric_limits<Value>::is_signed)
                low = -high;
            else
                low = 0;
        }
        else
        {
            low = applyVisitor(FieldVisitorConvertToNumber<Value>(), parameters_[2]);
        }
    }

    String getName() const override
    {
        return "anon_avg";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row, Arena *) const override
    {
        const auto & values = static_cast<const ColVecType &>(*columns[0]);
        this->data(place).sum += std::clamp(values.getData()[row], low, high);
        ++this->data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).sum += this->data(rhs).sum;
        this->data(place).count += this->data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).sum, buf);
        writeVarUInt(this->data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).sum, buf);
        readVarUInt(this->data(place).count, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        Float64 noisy_sum = static_cast<Float64>(this->data(place).sum)
                + sampleLaplace() * (static_cast<Float64>(high) - static_cast<Float64>(low)) / 2 / this->eps;
        Float64 noisy_count = this->data(place).count / this->eps;
        dynamic_cast<ColumnFloat64 &>(to).getData().push_back(noisy_sum / noisy_count);
    }

private:
    Value low, high; // Bounds
};


template <typename Value>
struct AggregateFunctionSumDP : public AllSavingAggregation<Value> {
    using ResultType = SumAccumulatorType<Value>;
    using ResultDataType = std::conditional_t<IsDecimalNumber<Value>, DataTypeDecimal<ResultType>, DataTypeNumber<ResultType>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<Value>, ColumnDecimal<Value>, ColumnVector<Value>>;


    AggregateFunctionSumDP(const DataTypes & argument_types_, const Array & parameters_) :
        AllSavingAggregation<Value>(argument_types_, parameters_)
    {}

    String getName() const override
    {
        return "anon_sum";
    }

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimalNumber<Value>)
            return std::make_shared<ResultDataType>(DecimalUtils::maxPrecision<Value>(), this->scale);
        else
            return std::make_shared<ResultDataType>();
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        ResultType res = 0;
        auto & array = this->data(place).array;
        Value public_abs = PrivateMaxAbs<Value>::compute(array.begin(), array.end(), this->eps);
        for (const auto & item : array)
        {
            if (std::numeric_limits<Value>::is_signed || IsDecimalNumber<Value>)
                res += std::clamp(item, Value(-public_abs), public_abs);
            else
                res += std::clamp(item, Value(0), public_abs);
        }
        res += sampleLaplace() * static_cast<Float64>(public_abs) / this->eps;
        auto & column = static_cast<ColVecResult &>(to);
        column.getData().push_back(res);
    }
};

template <typename Value>
struct AggregateFunctionAvgDP : public AllSavingAggregation<Value> {
    AggregateFunctionAvgDP(const DataTypes & argument_types_, const Array & parameters_) :
            AllSavingAggregation<Value>(argument_types_, parameters_)
    {}

    String getName() const override
    {
        return "anon_avg";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        SumAccumulatorType<Value> sum = 0;
        auto & array = this->data(place).array;
        Value public_abs = PrivateMaxAbs<Value>::compute(array.begin(), array.end(), this->eps);
        for (const auto & item : array)
        {
            if (std::numeric_limits<Value>::is_signed || IsDecimalNumber<Value>)
            {
                sum += std::clamp(item, Value(-public_abs), public_abs);
            }
            else
            {
                sum += std::clamp(item, Value(0), public_abs);
            }
        }
        Float64 noisy_sum = static_cast<Float64>(sum) + sampleLaplace() * static_cast<Float64>(public_abs) / this->eps;
        Float64 noisy_count = array.size() + sampleLaplace() / this->eps;
        dynamic_cast<ColumnFloat64 &>(to).getData().push_back(noisy_sum / noisy_count);
    }
};



template <typename Value>
struct AggregateFunctionQuantileDP : public AllSavingAggregation<Value> {

    using ColVecType = std::conditional_t<IsDecimalNumber<Value>, ColumnDecimal<Value>, ColumnVector<Value>>;
    using ResultDataType = std::conditional_t<IsDecimalNumber<Value>, DataTypeDecimal<Value>, DataTypeNumber<Value>>;

    AggregateFunctionQuantileDP(const DataTypes & argument_types_, const Array & parameters_) :
        AllSavingAggregation<Value>(argument_types_, parameters_)
        , level(applyVisitor(FieldVisitorConvertToNumber<Float32>(), this->parameters[2]))
    {}

    AggregateFunctionQuantileDP(const DataTypes & argument_types_, const Array & parameters_, Float32 level_) :
        AllSavingAggregation<Value>(argument_types_, parameters_)
        , level(level_)
    {}

    String getName() const override
    {
        return "anon_quantile";
    }

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimalNumber<Value>)
            return std::make_shared<ResultDataType>(DecimalUtils::maxPrecision<Value>(), this->scale);
        else
            return std::make_shared<ResultDataType>();
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        Value left, right;
        int steps_limit = BinsHelper<Value>::bins();
        if (this->parameters.size() == 4)
        {
            left = applyVisitor(FieldVisitorConvertToNumber<Value>(), this->parameters[2]);
            right = applyVisitor(FieldVisitorConvertToNumber<Value>(), this->parameters[3]);
        }
        else
        {
            if (IsDecimalNumber<Value>)
            {
                left = std::numeric_limits<typename NativeType<Value>::Type>::lowest();
                right = std::numeric_limits<typename NativeType<Value>::Type>::max();
            }
            else
            {
                left = std::numeric_limits<Value>::lowest();
                right = std::numeric_limits<Value>::max();
            }
        }

        this->data(place).sort();
        auto & array = this->data(place).array;

        Value prev_mid = left; // for early stop
        for (int step = 0; step < steps_limit; ++step)
        {
            Value mid = (left + right) / 2;
            if (mid == prev_mid) break;
            prev_mid = mid;

            size_t less_than_mid = std::upper_bound(array.begin(), array.end(), mid) - array.begin();
            if (less_than_mid + sampleLaplace() / this->eps > array.size() * level)
                right = mid;
            else
                left = mid;
        }
        static_cast<ColVecType &>(to).getData().push_back(right);
    }

private:
    Float32 level;
};

/**
 * @tparam Value - type of values
 * @tparam binary_bins - use bins of power of 2
 * @tparam bins_count
 */
template <typename Value>
struct PrivateMaxAbs
{
    /**
     * Computes privately max abs
     * [itb, ita) - range
     * @param failure_probability - probability, that no borders will be found and assumed to be 1.
     *                              low values result in more clipping.
     * @param eps - key Differential Privacy parameter
     * @return privately computed max abs; for decimals returns like underlying type
     */
     template <typename Iter>
    static Value compute(Iter itb, Iter ite, Float64 eps, Float64 failure_probability = 1e7)
    {
        std::vector<size_t> bins(BinsHelper<Value>::bins());
        while (itb != ite)
        {
            ++bins[BinsHelper<Value>::getBin(*itb)];
            ++itb;
        }
        size_t threshold = -log(1 - std::pow(failure_probability, 1.0 / (BinsHelper<Value>::bins() - 1))) / eps;
        for (int i = BinsHelper<Value>::bins() - 1; i > 0; --i)
        {
            if (bins[i] + sampleLaplace() / eps >= threshold)
                return BinsHelper<Value>::getHighBorder(i);
        }
        return BinsHelper<Value>::getHighBorder(0);
    }
};

template <typename Value>
struct BinsHelper<Value, true> {
    static int bins()
    {
        return sizeof(Value) * 8;
    }

    static int getBin(const Value & val)
    {
        if (unlikely(val == 0)) // Now never occurs
            return 0;
        // Cause abs(INT_MIN) is UB
        if (val == std::numeric_limits<Value>::min())
            return sizeof(Value) * 8 - 1;
        return std::log2(abs(val));
    }

    static Value getHighBorder(int bin)
    {
        if (bin == bins() - 1)
            return std::numeric_limits<Value>::max();
        return pow(2, bin);
    }
};

template <typename Value>
struct BinsHelper<Value, false> {
    static int getBin(const Value & val)
    {
        if (unlikely(val == 0)) // Now never occurs
            return 0;
        return round(std::log2(abs(val))) - minLog();
    }

    static int bins()
    {
       return getBin(std::numeric_limits<Value>::max()) + 1;
    }

    static Value getHighBorder(int bin)
    {
        if (bin == bins() - 1)
            return std::numeric_limits<Value>::max();
        return pow(2, bin + minLog() + 0.5);
    }

private:
    static int minLog()
    {
        return round(std::log2(abs(std::numeric_limits<Value>::min())));
    }
};

template <typename Native>
struct BinsHelper<Decimal<Native>, false> {
    static int bins()
    {
        return sizeof(Native) * 8;
    }

    static int getBin(const Decimal<Native> & val)
    {
        if (unlikely(val == 0)) // Now never occurs
            return 0;
        // Cause abs(INT_MIN) is UB
        if (val.value == std::numeric_limits<Native>::min())
            return sizeof(Native) * 8 - 1;
        return round(std::log2(abs(val.value)));
    }

    static Decimal<Native> getHighBorder(int bin)
    {
        if (bin == bins() - 1)
            return std::numeric_limits<Native>::max();
        return pow(2, bin + 0.5);
    }
};

}