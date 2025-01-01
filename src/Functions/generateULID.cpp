#include "config.h"

#if USE_ULID

#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#include <ulid.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

class FunctionGenerateULID : public IFunction
{
public:
    static constexpr size_t ULID_LENGTH = 26;

    static constexpr auto name = "generateULID";

    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<FunctionGenerateULID>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 1)
            throw Exception(
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "Number of arguments for function {} doesn't match: passed {}, should be 0 or 1.",
                getName(), arguments.size());

        return std::make_shared<DataTypeFixedString>(ULID_LENGTH);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnFixedString::create(ULID_LENGTH);
        auto & vec_res = col_res->getChars();

        vec_res.resize(input_rows_count * ULID_LENGTH);

        ulid_generator generator;
        ulid_generator_init(&generator, 0);

        for (size_t offset = 0, size = vec_res.size(); offset < size; offset += ULID_LENGTH)
            ulid_generate(&generator, reinterpret_cast<char *>(&vec_res[offset]));

        return col_res;
    }
};


REGISTER_FUNCTION(GenerateULID)
{
    factory.registerFunction<FunctionGenerateULID>(FunctionDocumentation
    {
        .description=R"(
Generates a Universally Unique Lexicographically Sortable Identifier (ULID).
This function takes an optional argument, the value of which is discarded to generate different values in case the function is called multiple times.
The function returns a value of type FixedString(26).
)",
        .examples{
            {"ulid", "SELECT generateULID()", ""},
            {"multiple", "SELECT generateULID(1), generateULID(2)", ""}},
        .categories{"ULID"}
    });
}

}

#endif
