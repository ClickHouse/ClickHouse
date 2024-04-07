#pragma once

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/LowerUpperImpl.h>
#include <Interpreters/Context_fwd.h>
#include <iostream>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename Impl, typename Name, bool is_injective = false>
class FunctionStringToString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionStringToString>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return is_injective;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        if constexpr (IsLowerUpper<Impl>::value)
        {
            // std::cout << "is lower upper:" << arguments[0].column->use_count() << std::endl;
            // std::cout << StackTrace().toString() << std::endl;
            if (arguments[0].column->use_count() == 1)
            {
                // std::cout << "execute in place" << std::endl;
                MutableColumnPtr mutable_column = arguments[0].column->assumeMutable();
                if (ColumnString * col = typeid_cast<ColumnString *>(mutable_column.get()))
                {
                    Impl::vectorInPlace(col->getChars(), col->getOffsets());
                    return std::move(mutable_column);
                }
                else if (ColumnFixedString * col_fixed = typeid_cast<ColumnFixedString *>(mutable_column.get()))
                {
                    Impl::vectorFixedInPlace(col_fixed->getChars(), col_fixed->getN());
                    return std::move(mutable_column);
                }
                else
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", mutable_column->getName(), getName());
            }
        }

        const ColumnPtr column = arguments[0].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            return col_res;
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col_fixed->getN());
            Impl::vectorFixed(col_fixed->getChars(), col_fixed->getN(), col_res->getChars());
            return col_res;
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName());
    }
};

}
