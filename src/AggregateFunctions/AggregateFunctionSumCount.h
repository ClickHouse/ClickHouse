#pragma once

#include <type_traits>
#include <DataTypes/DataTypeTuple.h>
#include <AggregateFunctions/AggregateFunctionAvg.h>
#include <AggregateFunctions/AggregateFunctionSum.h>


namespace DB
{
template <typename T>
class AggregateFunctionSumCount final : public AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionSumCount<T>>
{
public:
    using Base = AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionSumCount<T>>;
    using Numerator = typename Base::Numerator;
    using ColVecType = ColumnVectorOrDecimal<T>;

    AggregateFunctionSumCount(const DataTypes & argument_types_, UInt32 num_scale_ = 0)
         : Base(argument_types_, num_scale_), scale(num_scale_) {}

    DataTypePtr getReturnType() const override
    {
        auto second_elem = std::make_shared<DataTypeUInt64>();
        return std::make_shared<DataTypeTuple>(DataTypes{getReturnTypeFirstElement(), std::move(second_elem)});
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const final
    {
        assert_cast<ColumnVectorOrDecimal<AvgFieldType<T>> &>((assert_cast<ColumnTuple &>(to)).getColumn(0)).getData().push_back(
            this->data(place).numerator);

        assert_cast<ColumnUInt64 &>((assert_cast<ColumnTuple &>(to)).getColumn(1)).getData().push_back(
            this->data(place).denominator);
    }

    void NO_SANITIZE_UNDEFINED add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        this->data(place).numerator += static_cast<const ColVecType &>(*columns[0]).getData()[row_num];
        ++this->data(place).denominator;
    }

    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena *, ssize_t if_argument_pos) const override
    {
        AggregateFunctionSumData<Numerator> sum_data;
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            sum_data.addManyConditional(column.getData().data(), flags.data(), batch_size);
            for (size_t i = 0; i < batch_size; i++)
                this->data(place).denominator += (flags[i] != 0);
        }
        else
        {
            sum_data.addMany(column.getData().data(), batch_size);
            this->data(place).denominator += batch_size;
        }
        this->data(place).numerator += sum_data.sum;
    }

    void addBatchSinglePlaceNotNull(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, const UInt8 * null_map, Arena *, ssize_t if_argument_pos)
        const override
    {
        AggregateFunctionSumData<Numerator> sum_data;
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if (if_argument_pos >= 0)
        {
            /// Merge the 2 sets of flags (null and if) into a single one. This allows us to use parallelizable sums when available
            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            auto final_flags = std::make_unique<UInt8[]>(batch_size);
            size_t used_value = 0;
            for (size_t i = 0; i < batch_size; ++i)
            {
                final_flags[i] = (!null_map[i]) & if_flags[i];
                used_value += (!null_map[i]) & if_flags[i];
            }

            sum_data.addManyConditional(column.getData().data(), final_flags.get(), batch_size);
            this->data(place).denominator += used_value;
        }
        else
        {
            sum_data.addManyNotNull(column.getData().data(), null_map, batch_size);
            for (size_t i = 0; i < batch_size; i++)
                this->data(place).denominator += (!null_map[i]);
        }
        this->data(place).numerator += sum_data.sum;
    }

    String getName() const final { return "sumCount"; }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        return false;
    }

#endif

private:
    UInt32 scale;

    auto getReturnTypeFirstElement() const
    {
        using FieldType = AvgFieldType<T>;

        if constexpr (!is_decimal<T>)
            return std::make_shared<DataTypeNumber<FieldType>>();
        else
        {
            using DataType = DataTypeDecimal<FieldType>;
            return std::make_shared<DataType>(DataType::maxPrecision(), scale);
        }
    }
};

}
