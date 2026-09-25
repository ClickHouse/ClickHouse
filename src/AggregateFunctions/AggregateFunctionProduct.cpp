#include <bit>
#include <memory>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeNullable.h>
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
    /// Use a signaling NaN payload as the empty-state sentinel. Arithmetic NaNs are quiet NaNs,
    /// so they cannot become empty again after a value has been accumulated.
    static constexpr UInt64 empty_bits = 0x7ff0000000000001ULL;
    static constexpr UInt64 quiet_nan_bit = 0x0008000000000000ULL;
    static constexpr UInt64 exponent_mask = 0x7ff0000000000000ULL;
    static constexpr UInt64 mantissa_mask = 0x000fffffffffffffULL;

    Float64 product = std::bit_cast<Float64>(empty_bits);

    static Float64 normalizeInput(Float64 value)
    {
        UInt64 value_bits = std::bit_cast<UInt64>(value);
        if (value_bits == empty_bits)
            value_bits |= quiet_nan_bit;
        return std::bit_cast<Float64>(value_bits);
    }

    static bool isNaNValue(Float64 value)
    {
        const UInt64 value_bits = std::bit_cast<UInt64>(value);
        return (value_bits & exponent_mask) == exponent_mask && (value_bits & mantissa_mask) != 0;
    }

    bool isEmpty() const { return std::bit_cast<UInt64>(product) == empty_bits; }
    bool isNullResult() const { return isEmpty() || isNaNValue(product); }

    void add(Float64 value)
    {
        if (isEmpty())
            product = value;
        else
            product *= value;
    }

    void addDefaultValues(size_t length)
    {
        if (length != 0)
            add(0.0);
    }

    void merge(const AggregateFunctionProductData & rhs)
    {
        if (rhs.isEmpty())
            return;

        if (isEmpty())
            product = rhs.product;
        else
            product *= rhs.product;
    }

    void write(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(product, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinaryLittleEndian(product, buf);
    }
};

static_assert(sizeof(AggregateFunctionProductData) == sizeof(Float64));

template <typename T>
class AggregateFunctionProduct final
    : public IAggregateFunctionDataHelper<AggregateFunctionProductData, AggregateFunctionProduct<T>>
{
private:
    using Data = AggregateFunctionProductData;
    using ColVecType = ColumnVectorOrDecimal<T>;

    static Float64 toFloat64(const T & value, UInt32 scale)
    {
        Float64 converted = 0;
        if constexpr (is_decimal<T>)
            converted = DecimalUtils::convertTo<Float64>(value, scale);
        else
            converted = static_cast<Float64>(value);

        return converted;
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
            if constexpr (is_floating_point<T>)
            {
                Float64 converted = toFloat64(value, scale);

                /// Multiplying a discarded NaN or Inf by an arithmetic mask is not safe.
                /// Select the bit pattern of the multiplicative identity instead.
                UInt64 value_bits = std::bit_cast<UInt64>(converted);

                constexpr UInt64 identity_bits = 0x3ff0000000000000ULL;
                const UInt64 mask = 0 - static_cast<UInt64>(keep != 0);
                value_bits = (value_bits & mask) | (identity_bits & ~mask);

                return std::bit_cast<Float64>(value_bits);
            }

            return keep ? toFloat64(value, scale) : 1.0;
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
        const bool was_empty = data.isEmpty();
        Float64 product = was_empty ? 1.0 : data.product;
        UInt8 has_value = static_cast<UInt8>(!was_empty);

        for (size_t i = row_begin; i < row_end; ++i)
        {
            const UInt8 keep_value = static_cast<UInt8>(keep(i) != 0);
            has_value |= keep_value;
            product *= selectValueOrOne(values[i], keep_value, decimal_scale);
        }

        if (has_value)
            data.product = product;
    }

public:
    static constexpr bool DateTime64Supported = false;
    static constexpr bool can_use_lookup_table8 = false;

    explicit AggregateFunctionProduct(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionProduct<T>>(
            argument_types_, {}, makeNullable(std::make_shared<DataTypeNumber<Float64>>()))
        , decimal_scale(is_decimal<T> ? getDecimalScale(*argument_types_[0]) : 0)
    {
    }

    String getName() const override { return "product"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        Float64 value = toFloat64(column.getData()[row_num], decimal_scale);
        if constexpr (is_floating_point<T>)
            value = Data::normalizeInput(value);
        this->data(place).add(value);
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
        auto & nullable_column = assert_cast<ColumnNullable &>(to);
        const auto & data = this->data(place);

        if (data.isNullResult())
        {
            nullable_column.insertDefault();
            return;
        }

        assert_cast<ColumnVector<Float64> &>(nullable_column.getNestedColumn()).getData().push_back(data.product);
        nullable_column.getNullMapData().push_back(false);
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

The function aggregates rows directly and keeps a constant-size state. Input values of all supported
types are converted to `Float64` before multiplication. Consequently, the result can depend on
aggregation order for any input type, especially when data is processed in parallel. For wide integer
and Decimal input, results can differ from
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
        "Returns the product of the input values as a `Float64`. Returns `NULL` for empty input or if the product is `NaN`.",
        {"Float64", "NULL"}
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

    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("product", {createAggregateFunctionProduct, documentation, properties}, AggregateFunctionFactory::Case::Insensitive);
}

}
