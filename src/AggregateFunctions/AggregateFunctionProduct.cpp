#include <cstring>
#include <memory>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>

#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

struct AggregateFunctionProductData
{
    Float64 product = 1;
    UInt8 has_value = 0;

    void add(Float64 value)
    {
        product *= value;
        has_value = 1;
    }

    void addDefaultValues(size_t length)
    {
        if (length == 0)
            return;

        product *= 0.0;
        has_value = 1;
    }

    void merge(const AggregateFunctionProductData & rhs)
    {
        product *= rhs.product;
        has_value |= rhs.has_value;
    }

    void write(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(product, buf);
        writeBinaryLittleEndian(has_value, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinaryLittleEndian(product, buf);
        readBinaryLittleEndian(has_value, buf);
    }
};

static_assert(sizeof(AggregateFunctionProductData) <= 16);

template <typename T>
class AggregateFunctionProduct final
    : public IAggregateFunctionDataHelper<AggregateFunctionProductData, AggregateFunctionProduct<T>>
{
private:
    using Data = AggregateFunctionProductData;
    using ColVecType = ColumnVectorOrDecimal<T>;

    static Float64 toFloat64(const T & value, UInt32 scale)
    {
        if constexpr (is_decimal<T>)
            return DecimalUtils::convertTo<Float64>(value, scale);
        else
            return static_cast<Float64>(value);
    }

    static Float64 selectValueOrOne(const T & value, UInt8 keep, UInt32 scale)
    {
        if constexpr (is_decimal<T>)
        {
            if (!keep)
                return 1.0;

            return toFloat64(value, scale);
        }
        else
        {
            Float64 converted = toFloat64(value, scale);

            if constexpr (is_floating_point<T>)
            {
                /// Multiplying a discarded NaN or Inf by an arithmetic mask is not safe.
                /// Select the bit pattern of the multiplicative identity instead.
                UInt64 value_bits;
                std::memcpy(&value_bits, &converted, sizeof(converted));

                constexpr UInt64 identity_bits = 0x3ff0000000000000ULL;
                const UInt64 mask = 0 - static_cast<UInt64>(keep != 0);
                value_bits = (value_bits & mask) | (identity_bits & ~mask);

                std::memcpy(&converted, &value_bits, sizeof(converted));
                return converted;
            }

            return keep ? converted : 1.0;
        }
    }

    template <typename Keep>
    void addBatchWithKeep(
        AggregateDataPtr __restrict place,
        const T * __restrict values,
        size_t row_begin,
        size_t row_end,
        const Keep & keep) const
    {
        auto & data = this->data(place);
        Float64 product = data.product;
        UInt8 has_value = data.has_value;

        for (size_t i = row_begin; i < row_end; ++i)
        {
            const UInt8 keep_value = static_cast<UInt8>(keep(i) != 0);
            has_value |= keep_value;
            product *= selectValueOrOne(values[i], keep_value, decimal_scale);
        }

        data.product = product;
        data.has_value = has_value;
    }

public:
    static constexpr bool DateTime64Supported = false;

    explicit AggregateFunctionProduct(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T>>(
            argument_types_, {}, std::make_shared<DataTypeNumber<Float64>>())
        , decimal_scale(is_decimal<T> ? getDecimalScale(*argument_types_[0]) : 0)
    {
    }

    String getName() const override { return "product"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        this->data(place).add(toFloat64(column.getData()[row_num], decimal_scale));
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        const auto * values = column.getData().data();

        if (if_argument_pos >= 0)
        {
            const auto * flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            addBatchWithKeep(place, values, row_begin, row_end, [&](size_t i) { return flags[i]; });
        }
        else
        {
            addBatchWithKeep(place, values, row_begin, row_end, [](size_t) { return UInt8(1); });
        }
    }

    void addBatchSparseSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena) const override
    {
        const auto & column_sparse = assert_cast<const ColumnSparse &>(*columns[0]);
        const auto * values = &column_sparse.getValuesColumn();
        auto offset_it = column_sparse.getIterator(row_begin);

        for (size_t row = row_begin; row < row_end; ++row, ++offset_it)
            add(place, &values, offset_it.getValueIndex(), arena);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        const auto * values = column.getData().data();

        if (if_argument_pos >= 0)
        {
            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            addBatchWithKeep(place, values, row_begin, row_end, [&](size_t i)
            {
                return static_cast<UInt8>(!null_map[i]) & static_cast<UInt8>(!!if_flags[i]);
            });
        }
        else
        {
            addBatchWithKeep(place, values, row_begin, row_end, [&](size_t i) { return static_cast<UInt8>(!null_map[i]); });
        }
    }

    void addManyDefaults(
        AggregateDataPtr __restrict place,
        const IColumn **,
        size_t length,
        Arena *) const override
    {
        this->data(place).addDefaultValues(length);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<Float64> &>(to).getData().push_back(
            this->data(place).has_value ? this->data(place).product : 0.0);
    }

private:
    UInt32 decimal_scale;
};

AggregateFunctionPtr createAggregateFunctionProduct(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    const DataTypePtr & data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFunctionProduct>(*data_type, argument_types));
    else
        res.reset(createWithNumericType<AggregateFunctionProduct>(*data_type, argument_types));

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function {}{}",
            data_type->getName(), name, getNumericVariantSupertypeHint(data_type));

    return res;
}

}

void registerAggregateFunctionProduct(AggregateFunctionFactory & factory);
void registerAggregateFunctionProduct(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the product of numeric values.

The function aggregates rows directly and keeps a constant-size state. Input values are converted
to `Float64` before multiplication. Floating-point results can depend on aggregation order when
data is processed in parallel. For wide integer and Decimal input, results can differ from
`arrayProduct(groupArray(x))` because that expression can use a wider intermediate type. If the
input is already an array, use
[`arrayProduct`](/reference/functions/regular-functions/array-functions#arrayProduct) instead.
    )";
    FunctionDocumentation::Syntax syntax = "product(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Input values.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the product of the input values as a `Float64`. Returns `0` for an empty non-nullable input.",
        {"Float64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage",
        R"(
SELECT product(x)
FROM VALUES('x Float64', (1.5), (2), (4));
        )",
        R"(
┌─product(x)─┐
│          12 │
└─────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("product", {createAggregateFunctionProduct, documentation}, AggregateFunctionFactory::Case::Insensitive);
}

}
