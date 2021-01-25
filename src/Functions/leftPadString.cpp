#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionImpl.h>


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
    class FunctionLeftPadString : public IFunction
    {
    public:
        static constexpr auto name = "leftPadString";
        static FunctionPtr create(const Context &) { return std::make_shared<FunctionLeftPadString>(); }

        String getName() const override { return name; }

        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        bool useDefaultImplementationForConstants() const override { return true; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            size_t number_of_arguments = arguments.size();

            if (number_of_arguments != 3)
                throw Exception(
                    "Number of arguments for function " + getName() + " doesn't match: passed " + toString(number_of_arguments)
                        + ", should be 3",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            if (!isStringOrFixedString(arguments[0]))
                throw Exception(
                    "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (!isNativeNumber(arguments[1]))
                throw Exception(
                    "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (!isStringOrFixedString(arguments[2]))
                throw Exception(
                    "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override { }
    };

}

void registerFunctionPrependString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLeftPadString>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("lpad", "leftPadString", FunctionFactory::CaseInsensitive);
}

}
