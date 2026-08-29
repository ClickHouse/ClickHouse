#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnArray.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <type_traits>

constexpr size_t AGGREGATE_FUNCTION_MOVING_MAX_ARRAY_SIZE = 0xFFFFFF;


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

template <typename T>
struct MovingData
{
    /// For easy serialization.
    static_assert(std::has_unique_object_representations_v<T> || is_floating_point<T>);

    using Accumulator = T;

    /// Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    Array value;    /// Prefix sums.
    T sum{};

    void NO_SANITIZE_UNDEFINED add(T val, Arena * arena)
    {
        sum += val;
        value.push_back(sum, arena);
    }
};

template <typename T>
struct MovingSumData : public MovingData<T>
{
    static constexpr auto name = "groupArrayMovingSum";

    T NO_SANITIZE_UNDEFINED get(size_t idx, UInt64 window_size) const
    {
        if (idx < window_size)
            return this->value[idx];
        return this->value[idx] - this->value[idx - window_size];
    }
};

template <typename T>
struct MovingAvgData : public MovingData<T>
{
    static constexpr auto name = "groupArrayMovingAvg";

    T NO_SANITIZE_UNDEFINED get(size_t idx, UInt64 window_size) const
    {
        if (idx < window_size)
            return this->value[idx] / T(window_size);
        return (this->value[idx] - this->value[idx - window_size]) / T(window_size);
    }
};


template <typename T, typename LimitNumElements, typename Data>
class MovingImpl final
    : public IAggregateFunctionDataHelper<Data, MovingImpl<T, LimitNumElements, Data>>
{
    static constexpr bool limit_num_elems = LimitNumElements::value;
    UInt64 window_size;

public:
    using ResultT = typename Data::Accumulator;

    using ColumnSource = ColumnVectorOrDecimal<T>;

    /// Probably for overflow function in the future.
    using ColumnResult = ColumnVectorOrDecimal<ResultT>;

    explicit MovingImpl(const DataTypePtr & data_type_, UInt64 window_size_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<Data, MovingImpl<T, LimitNumElements, Data>>({data_type_}, {}, createResultType(data_type_))
        , window_size(window_size_) {}

    String getName() const override { return Data::name; }

    static DataTypePtr createResultType(const DataTypePtr & argument)
    {
        return std::make_shared<DataTypeArray>(getReturnTypeElement(argument));
    }

    void NO_SANITIZE_UNDEFINED add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto value = static_cast<const ColumnSource &>(*columns[0]).getData()[row_num];
        this->data(place).add(static_cast<ResultT>(value), arena);
    }

    void NO_SANITIZE_UNDEFINED merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = this->data(place);
        auto & rhs_elems = this->data(rhs);

        size_t cur_size = cur_elems.value.size();

        if (rhs_elems.value.size())
            cur_elems.value.insert(rhs_elems.value.begin(), rhs_elems.value.end(), arena);

        for (size_t i = cur_size; i < cur_elems.value.size(); ++i)
        {
            cur_elems.value[i] += cur_elems.sum;
        }

        cur_elems.sum += rhs_elems.sum;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(value.data()), size * sizeof(value[0]));
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_MOVING_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", AGGREGATE_FUNCTION_MOVING_MAX_ARRAY_SIZE);

        if (size > 0)
        {
            auto & value = this->data(place).value;
            value.resize(size, arena);
            buf.readStrict(reinterpret_cast<char *>(value.data()), size * sizeof(value[0]));
            this->data(place).sum = value.back();
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        size_t size = data.value.size();

        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + size);

        if (size)
        {
            typename ColumnResult::Container & data_to = assert_cast<ColumnResult &>(arr_to.getData()).getData();

            for (size_t i = 0; i < size; ++i)
            {
                if (!limit_num_elems)
                {
                    data_to.push_back(data.get(i, size));
                }
                else
                {
                    data_to.push_back(data.get(i, window_size));
                }
            }
        }
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

private:
    static auto getReturnTypeElement(const DataTypePtr & argument)
    {
        if constexpr (!is_decimal<ResultT>)
            return std::make_shared<DataTypeNumber<ResultT>>();
        else
        {
            using Res = DataTypeDecimal<ResultT>;
            return std::make_shared<Res>(Res::maxPrecision(), getDecimalScale(*argument));
        }
    }
};


namespace
{

template <typename T, typename LimitNumberOfElements>
struct MovingSum
{
    using Data = MovingSumData<std::conditional_t<is_decimal<T>,
        std::conditional_t<sizeof(T) <= sizeof(Decimal128), Decimal128, Decimal256>,
        NearestFieldType<T>>>;
    using Function = MovingImpl<T, LimitNumberOfElements, Data>;
};

template <typename T, typename LimitNumberOfElements>
struct MovingAvg
{
    using Data = MovingAvgData<std::conditional_t<is_decimal<T>,
        std::conditional_t<sizeof(T) <= sizeof(Decimal128), Decimal128, Decimal256>,
        Float64>>;
    using Function = MovingImpl<T, LimitNumberOfElements, Data>;
};

template <typename T, typename LimitNumberOfElements> using MovingSumTemplate = typename MovingSum<T, LimitNumberOfElements>::Function;
template <typename T, typename LimitNumberOfElements> using MovingAvgTemplate = typename MovingAvg<T, LimitNumberOfElements>::Function;

template <template <typename, typename> class Function, typename HasLimit, typename DecimalArg, typename ... TArgs>
inline AggregateFunctionPtr createAggregateFunctionMovingImpl(const std::string & name, const DataTypePtr & argument_type, TArgs ... args)
{
    AggregateFunctionPtr res;

    if constexpr (DecimalArg::value)
        res.reset(createWithDecimalType<Function, HasLimit>(*argument_type, argument_type, std::forward<TArgs>(args)...));
    else
        res.reset(createWithNumericType<Function, HasLimit>(*argument_type, argument_type, std::forward<TArgs>(args)...));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                        argument_type->getName(), name);

    return res;
}

template <template <typename, typename> class Function>
AggregateFunctionPtr createAggregateFunctionMoving(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    bool limit_size = false;

    UInt64 max_elems = std::numeric_limits<UInt64>::max();

    if (parameters.empty())
    {
        // cumulative sum without parameter
    }
    else if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
               throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive integer", name);

        if ((type == Field::Types::Int64 && parameters[0].safeGet<Int64>() <= 0) ||
            (type == Field::Types::UInt64 && parameters[0].safeGet<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive integer", name);

        limit_size = true;
        max_elems = parameters[0].safeGet<UInt64>();
    }
    else
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of parameters for aggregate function {}, should be 0 or 1", name);

    const DataTypePtr & argument_type = argument_types[0];
    if (!limit_size)
    {
        if (isDecimal(argument_type))
            return createAggregateFunctionMovingImpl<Function, std::false_type, std::true_type>(name, argument_type);
        return createAggregateFunctionMovingImpl<Function, std::false_type, std::false_type>(name, argument_type);
    }

    if (isDecimal(argument_type))
        return createAggregateFunctionMovingImpl<Function, std::true_type, std::true_type>(name, argument_type, max_elems);
    return createAggregateFunctionMovingImpl<Function, std::true_type, std::false_type>(name, argument_type, max_elems);
}

}


void registerAggregateFunctionMoving(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    FunctionDocumentation::Description description_sum = R"(
Calculates the moving sum of input values.

The function can take the window size as a parameter. If left unspecified, the function takes the window size equal to the number of rows in the column.
    )";
    FunctionDocumentation::Syntax syntax_sum = R"(
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
    )";
    FunctionDocumentation::Arguments arguments_sum = {
        {"numbers_for_summing", "Expression resulting in a numeric data type value.", {"(U)Int*", "Float*", "Decimal"}},
    };
    FunctionDocumentation::Parameters parameters_sum = {
        {"window_size", "Size of the calculation window. If left unspecified, the function takes the window size equal to the number of rows in the column.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_sum = {"Returns an array of the same size and type as the input data.", {"Array"}};
    FunctionDocumentation::Examples examples_sum = {
    {
        "Usage example",
        R"(
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = Memory;

INSERT INTO t VALUES (1, 1.1, 1.10), (2, 2.2, 2.20), (4, 4.4, 4.40), (7, 7.77, 7.77);

SELECT
    groupArrayMovingSum(int) AS I,
    groupArrayMovingSum(float) AS F,
    groupArrayMovingSum(dec) AS D
FROM t;
        )",
        R"(
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,7,14] │ [1.1,3.3000002,7.7000003,15.47] │ [1.10,3.30,7.70,15.47] │
└────────────┴─────────────────────────────────┴────────────────────────┘
        )"
    },
    {
        "With window size",
        R"(
SELECT
    groupArrayMovingSum(2)(int) AS I,
    groupArrayMovingSum(2)(float) AS F,
    groupArrayMovingSum(2)(dec) AS D
FROM t;
        )",
        R"(
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,6,11] │ [1.1,3.3000002,6.6000004,12.17] │ [1.10,3.30,6.60,12.17] │
└────────────┴─────────────────────────────────┴────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_sum = {20, 1};
    FunctionDocumentation::Category category_sum = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_sum = {description_sum, syntax_sum, arguments_sum, parameters_sum, returned_value_sum, examples_sum, introduced_in_sum, category_sum};

    factory.registerFunction("groupArrayMovingSum", { createAggregateFunctionMoving<MovingSumTemplate>, documentation_sum, properties });

    FunctionDocumentation::Description description = R"(
Calculates the moving average of input values.

:::note
The function uses rounding towards zero.
It truncates the decimal places insignificant for the resulting data type.
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"numbers_for_summing", "Expression resulting in a numeric data type value.", {"(U)Int*", "Float*", "Decimal"}},
    };
    FunctionDocumentation::Parameters parameters = {
        {"window_size", "Size of the calculation window. If left unspecified, the function takes the window size equal to the number of rows in the column.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of the same size and type as the input data.", {"Array"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = Memory;

INSERT INTO t VALUES (1, 1.1, 1.10), (2, 2.2, 2.20), (4, 4.4, 4.40), (7, 7.77, 7.77);

SELECT
    groupArrayMovingAvg(int) AS I,
    groupArrayMovingAvg(float) AS F,
    groupArrayMovingAvg(dec) AS D
FROM t;
        )",
        R"(
┌─I─────────┬─F───────────────────────────────────┬─D─────────────────────┐
│ [0,0,1,3] │ [0.275,0.82500005,1.9250001,3.8675] │ [0.27,0.82,1.92,3.86] │
└───────────┴─────────────────────────────────────┴───────────────────────┘
        )"
    },
    {
        "With window size",
        R"(
SELECT
    groupArrayMovingAvg(2)(int) AS I,
    groupArrayMovingAvg(2)(float) AS F,
    groupArrayMovingAvg(2)(dec) AS D
FROM t;
        )",
        R"(
┌─I─────────┬─F────────────────────────────────┬─D─────────────────────┐
│ [0,1,3,5] │ [0.55,1.6500001,3.3000002,6.085] │ [0.55,1.65,3.30,6.08] │
└───────────┴──────────────────────────────────┴───────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("groupArrayMovingAvg", { createAggregateFunctionMoving<MovingAvgTemplate>, documentation, properties });
}

}
