#if defined(SANITIZE_COVERAGE)

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#include <base/coverage.h>


namespace DB
{

namespace
{

enum class Kind : uint8_t
{
    Current,
    Cumulative,
    All
};

/** If ClickHouse is build with coverage instrumentation, returns an array
  * of currently accumulated (`coverageCurrent`)
  * or accumulated since the startup (`coverageCumulative`)
  * or all possible (`coverageAll`) unique code addresses.
  */
class FunctionCoverage : public IFunction
{
private:
    Kind kind;

public:
    String getName() const override
    {
        return kind == Kind::Current ? "coverage" : "coverageAll";
    }

    explicit FunctionCoverage(Kind kind_) : kind(kind_)
    {
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isDeterministic() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto coverage_table = kind == Kind::Current
            ? getCurrentCoverage()
            : (kind == Kind::Cumulative
                ? getCumulativeCoverage()
                : getAllInstrumentedAddresses());

        auto column_addresses = ColumnUInt64::create();
        auto & data = column_addresses->getData();

        for (auto ptr : coverage_table)
            if (ptr)
                data.push_back(ptr);

        auto column_array = ColumnArray::create(
            std::move(column_addresses),
            ColumnArray::ColumnOffsets::create(1, data.size()));

        return ColumnConst::create(std::move(column_array), input_rows_count);
    }
};

}

REGISTER_FUNCTION(Coverage)
{
    factory.registerFunction("coverageCurrent", [](ContextPtr){ return std::make_shared<FunctionCoverage>(Kind::Current); },
        FunctionDocumentation
        {
            .description=R"(
This function is only available if ClickHouse was built with the SANITIZE_COVERAGE=1 option.

It returns an array of unique addresses (a subset of the instrumented points in code) in the code
encountered at runtime after the previous coverage reset (with the `SYSTEM RESET COVERAGE` query) or after server startup.

[example:functions]

The order of array elements is undetermined.

You can use another function, `coverageAll` to find all instrumented addresses in the code to compare and calculate the percentage.

You can process the addresses with the `addressToSymbol` (possibly with `demangle`) and `addressToLine` functions
to calculate symbol-level, file-level, or line-level coverage.

If you run multiple tests sequentially and reset the coverage with the `SYSTEM RESET COVERAGE` query between the tests,
you can obtain a coverage information for every test in isolation, to find which functions are covered by which tests and vise-versa.

By default, every *basic block* in the code is covered, which roughly means - a sequence of instructions without jumps,
e.g. a body of for loop without ifs, or a single branch of if.

See https://clang.llvm.org/docs/SanitizerCoverage.html for more information.
)",
            .examples{
                {"functions", "SELECT DISTINCT demangle(addressToSymbol(arrayJoin(coverageCurrent())))", ""}},
            .categories{"Introspection"}
        });

    factory.registerFunction("coverageCumulative", [](ContextPtr){ return std::make_shared<FunctionCoverage>(Kind::Cumulative); },
        FunctionDocumentation
        {
            .description=R"(
This function is only available if ClickHouse was built with the SANITIZE_COVERAGE=1 option.

It returns an array of unique addresses (a subset of the instrumented points in code) in the code
encountered at runtime after server startup.

In contrast to `coverageCurrent` it cannot be reset with the `SYSTEM RESET COVERAGE`.

See the `coverageCurrent` function for the details.
)",
            .categories{"Introspection"}
        });

    factory.registerFunction("coverageAll", [](ContextPtr){ return std::make_shared<FunctionCoverage>(Kind::All); },
        FunctionDocumentation
        {
            .description=R"(
This function is only available if ClickHouse was built with the SANITIZE_COVERAGE=1 option.

It returns an array of all unique addresses in the code instrumented for coverage
- all possible addresses that can appear in the result of the `coverage` function.

You can use this function, and the `coverage` function to compare and calculate the coverage percentage.

See the `coverageCurrent` function for the details.
)",
            .categories{"Introspection"}
        });
}

}

#endif
