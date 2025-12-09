#include <cstddef>
#include <memory>
#include <string>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnsDateTime.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <base/Decimal.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    }

/// This function specification https://iceberg.apache.org/spec/#truncate-transform-details
class FunctionIcebergHash : public IFunction
{

public:
    static inline const char * name = "icebergHash";

    explicit FunctionIcebergHash(ContextPtr)
    {
    }

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionIcebergHash>(context_);
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for function icebergHash: expected 1 argument");
        auto context = Context::getGlobalContextInstance();

        const auto & column = arguments[0].column;
        const auto & type = arguments[0].type;

        auto result_column = ColumnInt32::create(input_rows_count);
        auto & result_data = result_column->getData();

        WhichDataType which(type);

        if (isBool(type) || which.isInteger() || which.isDate())
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto value = column->getInt(i);
                result_data[i] = hashLong(value);
            }
        }
        else if (which.isFloat())
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto value = column->getFloat64(i);
                result_data[i] = hashLong(doubleToLongBits(value));
            }
        }
        else if (which.isStringOrFixedString())
        {
            auto murmur_result = FunctionFactory::instance()
                                     .get("murmurHash3_32", context)
                                     ->build(arguments)
                                     ->execute(arguments, std::make_shared<DataTypeUInt32>(), input_rows_count, false);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                result_data[i] = murmur_result->getUInt(i);
            }
        }
        else if (which.isUUID())
        {
            // Function toUnderType: UUID => toUInt128 doesn't work for some reason so we need to use toUInt128 clickhouse implementation
            ColumnPtr intermediate_representation = FunctionFactory::instance()
                                                        .get("toUInt128", context)
                                                        ->build(arguments)
                                                        ->execute(arguments, std::make_shared<DataTypeUInt128>(), input_rows_count, false);
            const ColumnConst * const_column = checkAndGetColumn<ColumnConst>(intermediate_representation.get());
            const IColumn & wrapper_column = const_column ? const_column->getDataColumn() : *intermediate_representation.get();
            const ColumnVector<UInt128> & uuid_column = checkAndGetColumn<const ColumnVector<UInt128> &>(wrapper_column);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                UInt128 value = uuid_column.getData()[i];
                result_data[i] = hashUnderlyingIntBigEndian(value, /*reduce_two_complement*/ false);
            }
        }
        else if (which.isDateTime64())
        {
            const ColumnConst * const_column = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
            const IColumn & wrapper_column = const_column ? const_column->getDataColumn() : *arguments[0].column.get();
            const auto & source_col = checkAndGetColumn<DataTypeDateTime64::ColumnType>(wrapper_column);
            const ColumnDateTime64 * decimal_column = &source_col;
            assert(decimal_column != nullptr);
            UInt32 scale = decimal_column->getScale();
            if ((scale != 6) && (scale != 9))
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Unsupported scale for DateTime64 in IcebergHash function. Supports only microseconds and nanoseconds.");
            }
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                    DateTime64 value = decimal_column->getElement(i);
                    Int64 value_int = value.convertTo<Int64>();
                    if (scale == 9)
                    {
                        value_int = value_int / 1000;
                }
                result_data[i] = hashLong(value_int);
            }
        }
        else if (which.isDecimal())
        {
            const ColumnConst * const_column = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
            const IColumn & wrapper_column = const_column ? const_column->getDataColumn() : *arguments[0].column.get();
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                UInt128 value;
                if (which.isDecimal32())
                {
                    const ColumnDecimal<Decimal32> * decimal_column = typeid_cast<const ColumnDecimal<Decimal32> *>(&wrapper_column);
                    value = decimal_column->getElement(i).value;
                }
                else if (which.isDecimal64())
                {
                    const ColumnDecimal<Decimal64> * decimal_column = typeid_cast<const ColumnDecimal<Decimal64> *>(&wrapper_column);
                    value = decimal_column->getElement(i).value;
                }
                else if (which.isDecimal128())
                {
                    const ColumnDecimal<Decimal128> * decimal_column = typeid_cast<const ColumnDecimal<Decimal128> *>(&wrapper_column);
                    value = decimal_column->getElement(i).value;
                }
                else if (which.isDecimal256())
                {
                    const ColumnDecimal<Decimal256> * decimal_column = typeid_cast<const ColumnDecimal<Decimal256> *>(&wrapper_column);
                    value = decimal_column->getElement(i).value;
                }
                else
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported data type `{}` for icebergHash", type->getName());
                }
                result_data[i] = hashUnderlyingIntBigEndian(value, /*reduce_two_complement*/ true);
            }
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported data type `{}` for icebergHash", type->getName());
        }
        return result_column;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    static Int32 hashLong(Int64 value)
    {
        std::array<char, 8> little_endian_representation;
        for (char & i : little_endian_representation)
        {
            i = static_cast<unsigned char>(value & 0xFF);
            value >>= 8;
        }
        return MurmurHash3Impl32::apply(little_endian_representation.data(), 8);
    }

    static Int32 hashUnderlyingIntBigEndian(UInt128 value, bool reduce_two_complement)
    {
        std::array<char, 16> big_endian_representation;
        size_t taken = 1;
        signed char prev = 0;
        for (size_t i = 0; i < 16; ++i)
        {
            signed char c = static_cast<signed char>(value & 0xFF);
            big_endian_representation[i] = c;
            value >>= 8;
            // Take minimum number of bytes to represent the value and sign bit
            if ((i == 0) || ((c != 0) && (c != -1)) || ((c & 0x80) != (prev & 0x80)))
            {
                taken = i + 1;
            }
            prev = c;
        }
        // Take all bytes
        if (!reduce_two_complement)
        {
            taken = 16;
        }
        std::reverse(big_endian_representation.begin(), big_endian_representation.begin() + taken);
        return MurmurHash3Impl32::apply(big_endian_representation.data(), taken);
    }

    static UInt64 doubleToLongBits(Float64 value)
    {
        if (std::isnan(value))
        {
            // Return a canonical NaN representation
            return 0x7ff8000000000000ULL;
        }

        // For other values, use a union to perform the bit-level conversion
        union
        {
            Float64 d;
            UInt64 bits;
        } converter;

        converter.d = value;
        if (converter.bits == 0x8000000000000000ULL)
        {
            // Handle -0.0 case
            return 0x0000000000000000ULL;
        }
        return converter.bits;
    }
};

REGISTER_FUNCTION(IcebergHash)
{
    FunctionDocumentation::Description description = R"(Implements the logic of the iceberg [hashing transform](https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements))";
    FunctionDocumentation::Syntax syntax = "icebergHash(value)";
    FunctionDocumentation::Arguments arguments =
    {
        {"value", "Source value to take the hash of", {"Integer", "Bool", "Decimal", "Float*", "String", "FixedString", "UUID", "Date", "Time", "DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a 32-bit Murmur3 hash, x86 variant, seeded with 0", {"Int32"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT icebergHash(1.0 :: Float32)", "-142385009"}};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Hash;
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIcebergHash>(documentation);
}

class FunctionIcebergBucket : public IFunction
{

public:
    static inline const char * name = "icebergBucket";

    explicit FunctionIcebergBucket(ContextPtr)
    {
    }

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionIcebergBucket>(context_);
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for function icebergBucket: expected 2 arguments");
        auto value = (*arguments[0].column)[0].safeGet<Int64>();
        if (value <= 0 || value > std::numeric_limits<Int32>::max())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Function IcebergBucket accepts only positive bucket size which is suitable to Int32");

        auto context = Context::getGlobalContextInstance();

        auto iceberg_hash_arguments = {arguments[1]};
        auto iceberg_hash_func = FunctionFactory::instance().get("icebergHash", context)->build(iceberg_hash_arguments);
        auto iceberg_hash_result_type = iceberg_hash_func->getResultType();
        auto iceberg_hash_result = iceberg_hash_func->execute(iceberg_hash_arguments, iceberg_hash_result_type, input_rows_count, false);

        auto iceberg_hash_result_with_type = ColumnWithTypeAndName(iceberg_hash_result, std::make_shared<DataTypeInt32>(), "");
        auto max_int_with_type = ColumnWithTypeAndName(
            std::make_shared<DataTypeInt32>()->createColumnConst(input_rows_count, std::numeric_limits<Int32>::max()),
            std::make_shared<DataTypeInt32>(),
            "");
        auto bitand_result_type = std::make_shared<DataTypeInt32>();
        auto bitand_result = FunctionFactory::instance().get("bitAnd", context)->build({iceberg_hash_result_with_type, max_int_with_type})->execute({iceberg_hash_result_with_type, max_int_with_type}, bitand_result_type, input_rows_count, false);

        ColumnWithTypeAndName bitand_result_with_type(bitand_result, bitand_result_type, "");
        auto modulo_column = ColumnWithTypeAndName(
            std::make_shared<DataTypeUInt32>()->createColumnConst(input_rows_count, static_cast<UInt32>(value)),
            std::make_shared<DataTypeUInt32>(),
            "");
        ColumnsWithTypeAndName modulo_arguments = {bitand_result_with_type, modulo_column};
        auto modulo_func = FunctionFactory::instance().get("positiveModulo", context)->build(modulo_arguments);
        return modulo_func->execute(modulo_arguments, std::make_shared<DataTypeUInt32>(), input_rows_count, false);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
};

REGISTER_FUNCTION(IcebergBucket)
{
    FunctionDocumentation::Description description
        = R"(Implements logic for the [iceberg bucket transform](https://iceberg.apache.org/spec/#bucket-transform-details.))";
    FunctionDocumentation::Syntax syntax = "icebergBucket(N, value)";
    FunctionDocumentation::Arguments arguments =
    {
        {"N", "The number of buckets, modulo.", {"const (U)Int*"}},
        {"value", "The source value to transform.", {"(U)Int*", "Bool", "Decimal", "Float*", "String", "FixedString", "UUID", "Date", "Time", "DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a 32-bit hash of the source value.", {"Int32"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT icebergBucket(5, 1.0 :: Float32)", "4"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIcebergBucket>(documentation);
}

}
