#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Common/thread_local_rng.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_STRING_SIZE;
}


/* Generate random string of specified length with fully random bytes(including zero). */
class FunctionRandomPrintableASCII : public IFunction
{
public:
    static constexpr auto name = "randomString";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRandomString>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                "Function " + getName() + " requires at least one argument: the size of resulting string",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() > 2)
            throw Exception(
                "Function " + getName() + " requires at most two arguments: the size of resulting string and optional disambiguation tag",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const IDataType & length_type = *arguments[0];
        if (!isNumber(length_type))
            throw Exception("First argument of function " + getName() + " must have numeric type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        // TODO
    }
};

void registerFunctionRandomString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandomString>();
}

}
