#pragma once

#include <Common/config.h>

#if USE_HASHIDSXX

#    include <hashids.h>

#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsNumber.h>
#    include <DataTypes/DataTypeString.h>
#    include <Functions/FunctionFactory.h>
#    include <Functions/FunctionHelpers.h>
#    include <Functions/IFunction.h>

#    include <functional>
#    include <initializer_list>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

// hashid(string, salt)
class FunctionHashID : public IFunction
{
public:
    static constexpr auto name = "hashid";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionHashID>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 1)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects at least one argument", getName());

        if (!isUnsignedInteger(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be unsigned integer, got {}",
                getName(),
                arguments[0].type->getName());

        if (arguments.size() > 1)
        {
            if (!isString(arguments[1].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument of function {} must be String, got {}",
                    getName(),
                    arguments[1].type->getName());
        }

        if (arguments.size() > 2)
        {
            if (!isUInt8(arguments[2].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Third argument of function {} must be UInt8, got {}",
                    getName(),
                    arguments[2].type->getName());
        }

        if (arguments.size() > 3)
        {
            if (!isString(arguments[3].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Fourth argument of function {} must be String, got {}",
                    getName(),
                    arguments[3].type->getName());
        }

        if (arguments.size() > 4)
        {
            throw Exception(
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "Function {} expect no more than three arguments (integer, salt, optional_alphabet), got {}",
                getName(),
                arguments.size());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & numcolumn = arguments[0].column;

        if (checkAndGetColumn<ColumnUInt8>(numcolumn.get()) || checkAndGetColumn<ColumnUInt16>(numcolumn.get())
            || checkAndGetColumn<ColumnUInt32>(numcolumn.get()) || checkAndGetColumn<ColumnUInt64>(numcolumn.get())
            || checkAndGetColumnConst<ColumnUInt8>(numcolumn.get()) || checkAndGetColumnConst<ColumnUInt16>(numcolumn.get())
            || checkAndGetColumnConst<ColumnUInt32>(numcolumn.get()) || checkAndGetColumnConst<ColumnUInt64>(numcolumn.get()))
        {
            std::string salt;
            UInt8 minLength = 0;
            std::string alphabet;

            if (arguments.size() >= 4)
            {
                const auto & alphabetcolumn = arguments[3].column;
                if (auto alpha_col = checkAndGetColumnConst<ColumnString>(alphabetcolumn.get()))
                {
                    alphabet = alpha_col->getValue<String>();
                    if (alphabet.find("\0") != std::string::npos)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Custom alphabet must not contain null character");
                }
            }
            else
                alphabet.assign(DEFAULT_ALPHABET);

            if (arguments.size() >= 3)
            {
                const auto & minlengthcolumn = arguments[2].column;
                if (auto min_length_col = checkAndGetColumnConst<ColumnUInt8>(minlengthcolumn.get()))
                    minLength = min_length_col->getValue<UInt8>();
            }

            if (arguments.size() >= 2)
            {
                const auto & saltcolumn = arguments[1].column;
                if (auto salt_col = checkAndGetColumnConst<ColumnString>(saltcolumn.get()))
                    salt = salt_col->getValue<String>();
            }

            hashidsxx::Hashids hash(salt, minLength, alphabet);

            auto col_res = ColumnString::create();

            std::string hashid;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                hashid.assign(hash.encode({numcolumn->getUInt(i)}));
                col_res->insertDataWithTerminatingZero(hashid.data(), hashid.size() + 1);
            }

            return col_res;
        }
        else
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of first argument of function hashid", ErrorCodes::ILLEGAL_COLUMN);
    }
};

}

#endif
