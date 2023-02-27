#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** The function takes arguments x1, x2, ... xn, y. All arguments are bool.
  * x arguments represents the fact that some category is true.
  *
  * It calculates how many times y was true and how many times y was false when every n-th category was true
  * and the total number of times y was true and false.
  *
  * So, the size of the state is (n + 1) * 2 cells.
  */
class AggregateFunctionCategoricalIV final : public IAggregateFunctionHelper<AggregateFunctionCategoricalIV>
{
private:
    using Counter = UInt64;
    size_t category_count;

    static Counter & counter(AggregateDataPtr __restrict place, size_t i, bool what)
    {
        return reinterpret_cast<Counter *>(place)[i * 2 + (what ? 1 : 0)];
    }

    static const Counter & counter(ConstAggregateDataPtr __restrict place, size_t i, bool what)
    {
        return reinterpret_cast<const Counter *>(place)[i * 2 + (what ? 1 : 0)];
    }

public:
    AggregateFunctionCategoricalIV(const DataTypes & arguments_, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionCategoricalIV>{arguments_, params_, createResultType()}
        , category_count{arguments_.size() - 1}
    {
        // notice: argument types has been checked before
    }

    String getName() const override
    {
        return "categoricalInformationValue";
    }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override
    {
        memset(place, 0, sizeOfData());
    }

    void destroy(AggregateDataPtr __restrict) const noexcept override
    {
        // nothing
    }

    bool hasTrivialDestructor() const override
    {
        return true;
    }

    size_t sizeOfData() const override
    {
        return sizeof(Counter) * (category_count + 1) * 2;
    }

    size_t alignOfData() const override
    {
        return alignof(Counter);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto * y_col = static_cast<const ColumnUInt8 *>(columns[category_count]);
        bool y = y_col->getData()[row_num];

        for (size_t i = 0; i < category_count; ++i)
        {
            const auto * x_col = static_cast<const ColumnUInt8 *>(columns[i]);
            bool x = x_col->getData()[row_num];

            if (x)
                ++counter(place, i, y);
        }

        ++counter(place, category_count, y);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        for (size_t i = 0; i <= category_count; ++i)
        {
            counter(place, i, false) += counter(rhs, i, false);
            counter(place, i, true) += counter(rhs, i, true);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        buf.write(place, sizeOfData());
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        buf.readStrict(place, sizeOfData());
    }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeNumber<Float64>>());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override /// NOLINT
    {
        auto & col = static_cast<ColumnArray &>(to);
        auto & data_col = static_cast<ColumnFloat64 &>(col.getData());
        auto & offset_col = static_cast<ColumnArray::ColumnOffsets &>(col.getOffsetsColumn());

        data_col.reserve(data_col.size() + category_count);

        Float64 sum_no = static_cast<Float64>(counter(place, category_count, false));
        Float64 sum_yes = static_cast<Float64>(counter(place, category_count, true));

        for (size_t i = 0; i < category_count; ++i)
        {
            Float64 no = static_cast<Float64>(counter(place, i, false));
            Float64 yes = static_cast<Float64>(counter(place, i, true));

            data_col.insertValue((no / sum_no - yes / sum_yes) * (log((no / sum_no) / (yes / sum_yes))));
        }

        offset_col.insertValue(data_col.size());
    }
};


namespace
{

AggregateFunctionPtr createAggregateFunctionCategoricalIV(
    const std::string & name,
    const DataTypes & arguments,
    const Array & params,
    const Settings *)
{
    assertNoParameters(name, params);

    if (arguments.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires two or more arguments",
            name);

    for (const auto & argument : arguments)
    {
        if (!WhichDataType(argument).isUInt8())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "All the arguments of aggregate function {} should be UInt8",
                name);
    }

    return std::make_shared<AggregateFunctionCategoricalIV>(arguments, params);
}

}

void registerAggregateFunctionCategoricalIV(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };
    factory.registerFunction("categoricalInformationValue", { createAggregateFunctionCategoricalIV, properties });
}

}
