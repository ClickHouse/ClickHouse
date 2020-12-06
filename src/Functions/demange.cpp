#include <common/demangle.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Access/AccessFlags.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionDemangle : public IFunction
{
public:
    static constexpr auto name = "demangle";
    static FunctionPtr create(const Context & context)
    {
        context.checkAccess(AccessType::demangle);
        return std::make_shared<FunctionDemangle>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Function " + getName() + " needs exactly one argument; passed "
                + toString(arguments.size()) + ".", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto & type = arguments[0].type;

        if (!WhichDataType(type.get()).isString())
            throw Exception("The only argument for function " + getName() + " must be String. Found "
                + type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;
        const ColumnString * column_concrete = checkAndGetColumn<ColumnString>(column.get());

        if (!column_concrete)
            throw Exception("Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        auto result_column = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef source = column_concrete->getDataAtWithTerminatingZero(i);
            auto demangled = tryDemangle(source.data);
            if (demangled)
            {
                result_column->insertDataWithTerminatingZero(demangled.get(), strlen(demangled.get()) + 1);
            }
            else
            {
                result_column->insertDataWithTerminatingZero(source.data, source.size);
            }
        }

        return result_column;
    }
};

}

void registerFunctionDemangle(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDemangle>();
}

}

