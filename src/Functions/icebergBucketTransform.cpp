#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include "Columns/ColumnsDateTime.h"
#include "Core/Field.h"
#include "Core/Types.h"
#include "DataTypes/DataTypeDateTime64.h"
#include "base/Decimal.h"
#include "base/types.h"

#include "Common/logger_useful.h"

#include "base/wide_integer_to_string.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// This function specification https://iceberg.apache.org/spec/#truncate-transform-details
class FunctionIcebergHash : public IFunction
{

public:
    static inline const char * name = "icebergHash";


    using U = NearestFieldType<std::decay_t<DecimalField<Decimal64>>>;

    static_assert(std::is_same<U, DecimalField<Decimal64>>());

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
        auto context = Context::getGlobalContextInstance();

        const auto & column = arguments[0].column;
        const auto & type = arguments[0].type;

        auto result_column = ColumnInt32::create(input_rows_count);
        auto & result_data = result_column->getData();

        WhichDataType which(type);

        if (which.isInteger())
        {
            // Handle integer types
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto value = column->getInt(i);
                result_data[i] = hashLong(value);
            }
        }
        else if (which.isFloat())
        {
            // Handle floating-point types
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
            // Handle UUID types
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const ColumnConst * const_column = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
                const IColumn & wrapper_column = const_column ? const_column->getDataColumn() : *arguments[0].column.get();
                const ColumnVector<UUID> & uuid_column = checkAndGetColumn<const ColumnVector<UUID> &>(wrapper_column);
                UUID value = uuid_column.getData()[i];
                result_data[i] = hashUUID(value);
            }
        }
        else if (which.isDate()) 
        {
            // Handle date types
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto value = column->getInt(i);
                result_data[i] = hashLong(value);
            }
        }
        else if (which.isDateTime64())
        {
            // Handle datetime64 types
            const ColumnConst * const_column = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
            const IColumn & wrapper_column = const_column ? const_column->getDataColumn() : *arguments[0].column.get();
            const auto & source_col = checkAndGetColumn<DataTypeDateTime64::ColumnType>(wrapper_column);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                LOG_DEBUG(
                    &Poco::Logger::get("FunctionIcebergHash"), "column type: {}, family: {}", column->getName(), column->getFamilyName());
                const ColumnDateTime64 * decimal_column = &source_col;
                assert(decimal_column != nullptr);
                DateTime64 value = decimal_column->getElement(i);
                UInt32 scale = decimal_column->getScale();
                LOG_DEBUG(&Poco::Logger::get("FunctionIcebergHash"), "scale: {}", scale);
                LOG_DEBUG(&Poco::Logger::get("static_cast<const DataTypeDateTime64 *>(type.get())->getScale()"), "scale: {}", scale);
                assert(scale == 6 || scale == 9);
                Int64 value_int = value.convertTo<Int64>();
                if (scale == 9) {
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
                if (which.isDecimal32()) {
                    const ColumnDecimal<Decimal32> * decimal_column = typeid_cast<const ColumnDecimal<Decimal32> *>(&wrapper_column);
                    value = decimal_column->getElement(i).value;
                } else if (which.isDecimal64()) {
                    const ColumnDecimal<Decimal64> * decimal_column = typeid_cast<const ColumnDecimal<Decimal64> *>(&wrapper_column);
                    value = decimal_column->getElement(i).value;
                } else if (which.isDecimal128()) {
                    const ColumnDecimal<Decimal128> * decimal_column = typeid_cast<const ColumnDecimal<Decimal128> *>(&wrapper_column);
                    value = decimal_column->getElement(i).value;
                }
                result_data[i] = hashDecimalBase(value, /*take_all*/ false);
            }
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported data type for icebergHash");
        }
        return result_column;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:

    static Int32 hashLong(Int64 value) {
        char little_endian_representation[8];
        for (char & i : little_endian_representation)
        {
            i = static_cast<char>(value & 0xFF);
            value >>= 8;
        }
        return MurmurHash3Impl32::apply(little_endian_representation, 8);
    }

    static Int32 hashUUID(UUID value)
    {
        UInt128 underlying_value = value.toUnderType();
        LOG_DEBUG(&Poco::Logger::get("UUID"), "underlying value: {}", wide::to_string(underlying_value));
        return hashDecimalBase(underlying_value, true);
    }

    static Int32 hashDecimalBase(UInt128 value, bool take_all)
    {
        LOG_DEBUG(&Poco::Logger::get("Kek"), "value: {}", wide::to_string(value));
        char big_endian_representation[16];
        UInt32 taken = 1;
        char prev = static_cast<unsigned char>(value & 0xFF);
        value >>= 8;
        big_endian_representation[0] = prev;
        for (int i = 1; i < 16; ++i) {
            char c = static_cast<unsigned char>(value & 0xFF);
            big_endian_representation[i] = c;
            value >>= 8;
            LOG_DEBUG(&Poco::Logger::get("Kek"), "i: {}, c: {}, prev: {}", i, static_cast<int>(c), static_cast<int>(prev));
            LOG_DEBUG(
                &Poco::Logger::get("Kek"),
                "first cond: {}, second cond: {}, third cond: {}",
                c != 0,
                c != -1,
                ((c & 0x80) != (prev & 0x80)));
            if (((c != 0) && (c != -1)) || ((c & 0x80) != (prev & 0x80)))
            {
                taken = i + 1;
            }
            prev = c;
        }
        if (take_all)
        {
            taken = 16;
        }
        for (size_t i = 0; i < (taken >> 1); ++i) {
            std::swap(big_endian_representation[i], big_endian_representation[taken - i - 1]);
        }
        LOG_DEBUG(&Poco::Logger::get("Kek"), "Taken: {}", taken);
        for (size_t i = 0; i < taken; ++i)
        {
            // char buffer[3]; // 2 characters + null terminator

            // [[maybe_unused]] int error = std::snprintf(buffer, sizeof(buffer), "%02X", big_endian_representation[i]);
            LOG_DEBUG(&Poco::Logger::get("Kek"), "{}th byte is {}", i, static_cast<int>(big_endian_representation[i]));
        }
        return MurmurHash3Impl32::apply(big_endian_representation, taken);
    }

    static UInt64 doubleToLongBits(Float64 value) {
        if (std::isnan(value)) {
            // Return a canonical NaN representation
            return 0x7ff8000000000000ULL;
        }
        
        // For other values, use a union to perform the bit-level conversion
        union {
            Float64 d;
            UInt64 bits;
        } converter;
        
        converter.d = value;
        if (converter.bits == 0x8000000000000000ULL) {
            // Handle -0.0 case
            return 0x0000000000000000ULL;
        }
        return converter.bits;
    }
};

REGISTER_FUNCTION(IcebergHash)
{
    FunctionDocumentation::Description description = R"(Implements logic of iceberg hashing transform: https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements.)";
    FunctionDocumentation::Syntax syntax = "icebergHash(N, value)";
    FunctionDocumentation::Arguments arguments = {{"value", "String, integer or Decimal value."}};
    FunctionDocumentation::ReturnedValue returned_value = "Int32";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT icebergHash(3, 'iceberg')", "ice"}};
    FunctionDocumentation::Category category = {"Other"};

    factory.registerFunction<FunctionIcebergHash>({description, syntax, arguments, returned_value, examples, category});
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
        auto value = (*arguments[0].column)[0].safeGet<Int64>();
        if (value <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function IcebergBucket accepts only positive width");

        auto context = Context::getGlobalContextInstance();

        auto iceberg_hash_arguments = {arguments[1]};
        auto iceberg_hash_func = FunctionFactory::instance().get("icebergHash", context)->build(iceberg_hash_arguments);
        auto iceberg_hash_result_type = iceberg_hash_func->getResultType();
        auto iceberg_hash_result = iceberg_hash_func->execute(iceberg_hash_arguments, iceberg_hash_result_type, input_rows_count, false);
        
        auto iceberg_hash_result_with_type = ColumnWithTypeAndName(iceberg_hash_result, std::make_shared<DataTypeInt32>(), "");
        auto max_int_with_type = ColumnWithTypeAndName(std::make_shared<DataTypeInt32>()->createColumnConstWithDefaultValue(std::numeric_limits<Int32>::max()), std::make_shared<DataTypeInt32>(), "");
        auto bitand_result_type = std::make_shared<DataTypeInt32>();
        auto bitand_result = FunctionFactory::instance().get("bitAnd", context)->build({iceberg_hash_result_with_type, max_int_with_type})->execute({iceberg_hash_result_with_type, max_int_with_type}, bitand_result_type, input_rows_count, false);

        ColumnWithTypeAndName bitand_result_with_type(bitand_result, bitand_result_type, "");
        ColumnsWithTypeAndName modulo_arguments = {iceberg_hash_result_with_type, arguments[0]};
        auto modulo_func = FunctionFactory::instance().get("positiveModulo", context)->build(modulo_arguments);
        auto modulo_result_type = modulo_func->getResultType();
        return modulo_func->execute(modulo_arguments, modulo_result_type, input_rows_count, false);
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
};

REGISTER_FUNCTION(IcebergBucket)
{
    FunctionDocumentation::Description description = R"(Implements logic of iceberg truncate transform: https://iceberg.apache.org/spec/#truncate-transform-details.)";
    FunctionDocumentation::Syntax syntax = "IcebergBucket(N, value)";
    FunctionDocumentation::Arguments arguments = {{"value", "String, integer or Decimal value."}};
    FunctionDocumentation::ReturnedValue returned_value = "The same type as argument";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT IcebergBucket(3, 'iceberg')", "ice"}};
    FunctionDocumentation::Category category = {"Other"};

    factory.registerFunction<FunctionIcebergBucket>({description, syntax, arguments, returned_value, examples, category});
}
}

}
