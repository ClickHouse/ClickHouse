#include <base/demangle.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Access/Common/AccessFlags.h>
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
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::demangle);
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

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} needs exactly one argument; passed {}.",
                getName(), arguments.size());

        const auto & type = arguments[0].type;

        if (!WhichDataType(type.get()).isString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The only argument for function {} must be String. "
                "Found {} instead.", getName(), type->getName());

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
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", column->getName(), getName());

        auto result_column = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRef source = column_concrete->getDataAt(i);
            auto demangled = tryDemangle(source.data);
            if (demangled)
            {
                result_column->insertData(demangled.get(), strlen(demangled.get()));
            }
            else
            {
                result_column->insertData(source.data, source.size);
            }
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(Demangle)
{
    factory.registerFunction<FunctionDemangle>();
}

}
