#pragma once

#include <cmath>

#include <base/arithmeticOverflow.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnArray.h>
#include "Columns/ColumnsNumber.h"
#include <Common/PODArray.h>
#include <Common/PODArray_fwd.h>


/** This is simple, not numerically stable
  *  implementations of variance/covariance/correlation functions.
  *
  * It is about two times faster than stable variants.
  * Numerical errors may occur during summation.
  *
  * This implementation is selected as default,
  *  because "you don't pay for what you don't need" principle.
  *
  * For more sophisticated implementation, look at AggregateFunctionStatistics.h
  */

namespace DB
{
struct Settings;

enum class StatisticsFunctionKind
{
    varPop, varSamp,
    stddevPop, stddevSamp,
    skewPop, skewSamp,
    kurtPop, kurtSamp,
    covarPop, covarSamp,
    corr
};


template <typename T, StatisticsFunctionKind _kind, size_t _level>
struct StatFuncOneArg
{
    using Type1 = T;
    using Type2 = T;
    using ResultType = std::conditional_t<std::is_same_v<T, Float32>, Float32, Float64>;
    using Data = std::conditional_t<is_decimal<T>, VarMomentsDecimal<Decimal128, _level>, VarMoments<ResultType, _level>>;

    static constexpr StatisticsFunctionKind kind = _kind;
    static constexpr UInt32 num_args = 1;
};

template <typename T1, typename T2, StatisticsFunctionKind _kind>
struct StatFuncTwoArg
{
    using Type1 = T1;
    using Type2 = T2;
    using ResultType = std::conditional_t<std::is_same_v<T1, T2> && std::is_same_v<T1, Float32>, Float32, Float64>;
    using Data = std::conditional_t<_kind == StatisticsFunctionKind::corr, CorrMoments<ResultType>, CovarMoments<ResultType>>;

    static constexpr StatisticsFunctionKind kind = _kind;
    static constexpr UInt32 num_args = 2;
};


template <typename StatFunc>
class AggregateFunctionVarianceSimple final
    : public IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>
{
public:
    using T1 = typename StatFunc::Type1;
    using T2 = typename StatFunc::Type2;
    using ColVecT1 = ColumnVectorOrDecimal<T1>;
    using ColVecT2 = ColumnVectorOrDecimal<T2>;
    using ResultType = typename StatFunc::ResultType;
    using ColVecResult = ColumnVector<ResultType>;

    explicit AggregateFunctionVarianceSimple(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>(argument_types_, {}, std::make_shared<DataTypeNumber<ResultType>>())
        , src_scale(0)
    {}

    AggregateFunctionVarianceSimple(const IDataType & data_type, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>(argument_types_, {}, std::make_shared<DataTypeNumber<ResultType>>())
        , src_scale(getDecimalScale(data_type))
    {}

    String getName() const override
    {
        if constexpr (StatFunc::kind == StatisticsFunctionKind::varPop)
            return "varPop";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::varSamp)
            return "varSamp";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevPop)
            return "stddevPop";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevSamp)
            return "stddevSamp";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::skewPop)
            return "skewPop";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::skewSamp)
            return "skewSamp";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtPop)
            return "kurtPop";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtSamp)
            return "kurtSamp";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::covarPop)
            return "covarPop";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::covarSamp)
            return "covarSamp";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::corr)
            return "corr";
        UNREACHABLE();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (StatFunc::num_args == 2)
            this->data(place).add(
                static_cast<ResultType>(static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num]),
                static_cast<ResultType>(static_cast<const ColVecT2 &>(*columns[1]).getData()[row_num]));
        else
        {
            if constexpr (is_decimal<T1>)
            {
                this->data(place).add(static_cast<ResultType>(
                    static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num].value));
            }
            else
                this->data(place).add(
                    static_cast<ResultType>(static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num]));
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        auto & dst = static_cast<ColVecResult &>(to).getData();

        if constexpr (is_decimal<T1>)
        {
            if constexpr (StatFunc::kind == StatisticsFunctionKind::varPop)
                dst.push_back(data.getPopulation(src_scale * 2));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::varSamp)
                dst.push_back(data.getSample(src_scale * 2));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevPop)
                dst.push_back(sqrt(data.getPopulation(src_scale * 2)));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevSamp)
                dst.push_back(sqrt(data.getSample(src_scale * 2)));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::skewPop)
            {
                Float64 var_value = data.getPopulation(src_scale * 2);

                if (var_value > 0)
                    dst.push_back(data.getMoment3(src_scale * 3) / pow(var_value, 1.5));
                else
                    dst.push_back(std::numeric_limits<Float64>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::skewSamp)
            {
                Float64 var_value = data.getSample(src_scale * 2);

                if (var_value > 0)
                    dst.push_back(data.getMoment3(src_scale * 3) / pow(var_value, 1.5));
                else
                    dst.push_back(std::numeric_limits<Float64>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtPop)
            {
                Float64 var_value = data.getPopulation(src_scale * 2);

                if (var_value > 0)
                    dst.push_back(data.getMoment4(src_scale * 4) / pow(var_value, 2));
                else
                    dst.push_back(std::numeric_limits<Float64>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtSamp)
            {
                Float64 var_value = data.getSample(src_scale * 2);

                if (var_value > 0)
                    dst.push_back(data.getMoment4(src_scale * 4) / pow(var_value, 2));
                else
                    dst.push_back(std::numeric_limits<Float64>::quiet_NaN());
            }
        }
        else
        {
            if constexpr (StatFunc::kind == StatisticsFunctionKind::varPop)
                dst.push_back(data.getPopulation());
            if constexpr (StatFunc::kind == StatisticsFunctionKind::varSamp)
                dst.push_back(data.getSample());
            if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevPop)
                dst.push_back(sqrt(data.getPopulation()));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevSamp)
                dst.push_back(sqrt(data.getSample()));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::skewPop)
            {
                ResultType var_value = data.getPopulation();

                if (var_value > 0)
                    dst.push_back(static_cast<ResultType>(data.getMoment3() / pow(var_value, 1.5)));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::skewSamp)
            {
                ResultType var_value = data.getSample();

                if (var_value > 0)
                    dst.push_back(static_cast<ResultType>(data.getMoment3() / pow(var_value, 1.5)));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtPop)
            {
                ResultType var_value = data.getPopulation();

                if (var_value > 0)
                    dst.push_back(static_cast<ResultType>(data.getMoment4() / pow(var_value, 2)));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtSamp)
            {
                ResultType var_value = data.getSample();

                if (var_value > 0)
                    dst.push_back(static_cast<ResultType>(data.getMoment4() / pow(var_value, 2)));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::covarPop)
                dst.push_back(data.getPopulation());
            if constexpr (StatFunc::kind == StatisticsFunctionKind::covarSamp)
                dst.push_back(data.getSample());
            if constexpr (StatFunc::kind == StatisticsFunctionKind::corr)
                dst.push_back(data.get());
        }
    }

private:
    UInt32 src_scale;
};


template <typename T> using AggregateFunctionVarPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::varPop, 2>>;
template <typename T> using AggregateFunctionVarSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::varSamp, 2>>;
template <typename T> using AggregateFunctionStddevPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::stddevPop, 2>>;
template <typename T> using AggregateFunctionStddevSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::stddevSamp, 2>>;
template <typename T> using AggregateFunctionSkewPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::skewPop, 3>>;
template <typename T> using AggregateFunctionSkewSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::skewSamp, 3>>;
template <typename T> using AggregateFunctionKurtPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::kurtPop, 4>>;
template <typename T> using AggregateFunctionKurtSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::kurtSamp, 4>>;
template <typename T1, typename T2> using AggregateFunctionCovarPopSimple = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, StatisticsFunctionKind::covarPop>>;
template <typename T1, typename T2> using AggregateFunctionCovarSampSimple = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, StatisticsFunctionKind::covarSamp>>;
template <typename T1, typename T2> using AggregateFunctionCorrSimple = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, StatisticsFunctionKind::corr>>;


template <StatisticsFunctionKind _kind>
struct StatFuncArbitraryArgData
{
    using DataType = std::conditional_t<_kind == StatisticsFunctionKind::corr, CorrMoments<Float64>, CovarMoments<Float64>>;

    StatFuncArbitraryArgData() = default;

    explicit StatFuncArbitraryArgData(const size_t _num_args)
        : num_args(_num_args)
    {
        data_matrix.resize_fill(num_args * (num_args + 1) / 2, DataType());
    }

    void add(const IColumn ** column, const size_t row_num)
    {
        for (size_t i = 0; i < num_args; ++i)
            for (size_t j = 0; j <= i; ++j) 
                 data_matrix[i * (i + 1) / 2 + j].add(column[i]->getFloat64(row_num), column[j]->getFloat64(row_num));
    }

    void merge(const StatFuncArbitraryArgData & other)
    {
        for (size_t i = 0; i < num_args; ++i)
            for (size_t j = 0; j <= i; ++j)
                data_matrix[i * (i + 1) / 2 + j].merge(other.data_matrix[i * (i + 1) / 2 + j]);
    }

    void serialize(WriteBuffer & buf) const
    {
        for (size_t i = 0; i < num_args; ++i)
            for (size_t j = 0; j <= i; ++j)
                data_matrix[i * (i + 1) / 2 + j].write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        for (size_t i = 0; i < num_args; ++i)
            for (size_t j = 0; j <= i; ++j)
                data_matrix[i * (i + 1) / 2 + j].read(buf);
    }

    void insertResultInto(IColumn & to) const
    {
        auto & data_to = assert_cast<ColumnFloat64 &>(assert_cast<ColumnArray &>(assert_cast<ColumnArray &>(to).getData()).getData()).getData();
        auto & root_offsets_to = assert_cast<ColumnArray &>(to).getOffsets();
        auto & nested_offsets_to = assert_cast<ColumnArray &>(assert_cast<ColumnArray &>(to).getData()).getOffsets();
        for (size_t i = 0; i < num_args; ++i)
        {
            for (size_t j = 0; j < num_args; ++j)
            {
                auto & data = i < j ? data_matrix[j * (j + 1) / 2 + i] : data_matrix[i * (i + 1) / 2 + j];
                if constexpr (kind == StatisticsFunctionKind::covarPop)
                    data_to.push_back(data.getPopulation());
                if constexpr (kind == StatisticsFunctionKind::covarSamp)
                    data_to.push_back(data.getSample());
                if constexpr (kind == StatisticsFunctionKind::corr)
                    data_to.push_back(data.get());
            }
            nested_offsets_to.push_back(nested_offsets_to.back() + num_args);
        }
        root_offsets_to.push_back(root_offsets_to.back() + num_args);
    }

    static constexpr StatisticsFunctionKind kind = _kind;
    PaddedPODArray<DataType> data_matrix;
    size_t num_args;
};

template <typename StatFuncData>
class AggregateFunctionVarianceSimpleMatrix final
    : public IAggregateFunctionDataHelper<StatFuncData, AggregateFunctionVarianceSimpleMatrix<StatFuncData>>
{
public:

    explicit AggregateFunctionVarianceSimpleMatrix(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<StatFuncData, AggregateFunctionVarianceSimpleMatrix<StatFuncData>>(argument_types_, {}, createResultType())
    {}

    AggregateFunctionVarianceSimpleMatrix(const IDataType &, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<StatFuncData, AggregateFunctionVarianceSimpleMatrix<StatFuncData>>(argument_types_, {}, createResultType())
    {}

    String getName() const override
    {
        if constexpr (StatFuncData::kind == StatisticsFunctionKind::covarPop)
            return "covarPopMatrix";
        if constexpr (StatFuncData::kind == StatisticsFunctionKind::covarSamp)
            return "covarSampMatrix";
        if constexpr (StatFuncData::kind == StatisticsFunctionKind::corr)
            return "corrMatrix";
        UNREACHABLE();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) StatFuncData(this->argument_types.size());
    }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>()));
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(columns, row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }
};

using AggregateFunctionCovarPopSimpleMatrix = AggregateFunctionVarianceSimpleMatrix<StatFuncArbitraryArgData<StatisticsFunctionKind::covarPop>>;
using AggregateFunctionCovarSampSimpleMatrix = AggregateFunctionVarianceSimpleMatrix<StatFuncArbitraryArgData<StatisticsFunctionKind::covarSamp>>;
using AggregateFunctionCorrSimpleMatrix = AggregateFunctionVarianceSimpleMatrix<StatFuncArbitraryArgData<StatisticsFunctionKind::corr>>;

}
