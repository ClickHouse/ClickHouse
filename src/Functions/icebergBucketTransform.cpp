#include <memory>
#include <type_traits>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include "Core/Field.h"
#include "Core/Types.h"
#include "DataTypes/DataTypeDateTime64.h"
#include "base/Decimal.h"
#include "base/types.h"
#include <Functions/FunctionsHashing.h>

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
        } else if (which.isStringOrFixedString())
        {
            FunctionFactory::instance().get("FunctionMurmurHash3_32", context)->build(arguments)->execute(arguments, std::make_shared<DataTypeInt32>(), input_rows_count, false);
        }
        else if (which.isUUID())
        {
            // Handle UUID types
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const ColumnVector<UUID> * uuid_column = static_cast<const ColumnVector<UUID> *>(column.get());
                UUID value = uuid_column->getData()[i];
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
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const ColumnDecimal<DateTime64> * decimal_column = static_cast<const ColumnDecimal<DateTime64> *>(column.get());
                DateTime64 value = decimal_column->getElement(i);
                UInt32 scale = decimal_column->getScale();
                assert(scale == static_cast<const DataTypeDateTime64 *>(type.get())->getScale());
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
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                UInt128 value;
                if (which.isDecimal32()) {
                    ColumnDecimal<Decimal32> * decimal_column = typeid_cast<ColumnDecimal<Decimal32> *>(column.get());
                    value = decimal_column->getElement(i).value;
                } else if (which.isDecimal64()) {
                    ColumnDecimal<Decimal64> * decimal_column = typeid_cast<ColumnDecimal<Decimal64> *>(column.get());
                    value = decimal_column->getElement(i).value;
                } else if (which.isDecimal128()) {
                    ColumnDecimal<Decimal128> * decimal_column = typeid_cast<ColumnDecimal<Decimal128> *>(column.get());
                    value = decimal_column->getElement(i).value;
                }
                result_data[i] = hashDecimalBase(value);
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

    static Int32 hashUUID(UUID value) {
        char big_endian_representation[16];
        UInt128 underlying_value = value.toUnderType();
        for (char & i : big_endian_representation)
        {
            i = static_cast<char>(underlying_value & 0xFF);
            underlying_value >>= 8;
        }
        for (size_t i = 0; i < (sizeof(big_endian_representation) >> 1); ++i)
        {
            std::swap(big_endian_representation[i], big_endian_representation[sizeof(big_endian_representation) - i - 1]);
        }
        return MurmurHash3Impl32::apply(big_endian_representation, sizeof(big_endian_representation));
    }

    static Int32 hashDecimalBase(UInt128 value) {
        char big_endian_representation[16];
        UInt32 taken = 1;
        char prev = value & 0xFF;
        value >>= 8;
        big_endian_representation[0] = prev;
        for (int i = 1; i < 16; ++i) {
            char c = value & 0xFF;
            big_endian_representation[i] = c;
            value >>= 8;
            if (c != prev) {
                taken = i;
            }
            if ((c != 0) && (c != -1)) {
                taken = i + 1;
            }
        }
        if ((taken < 16) && ((big_endian_representation[taken - 1] & 0xF0) != (prev & 0xF0))) {
            ++taken;
        }
        for (size_t i = 0; i < (taken >> 1); ++i) {
            std::swap(big_endian_representation[i], big_endian_representation[taken - i - 1]);
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
