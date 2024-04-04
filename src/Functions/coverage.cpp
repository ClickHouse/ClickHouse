#if defined(SANITIZE_COVERAGE)

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#include <base/coverage.h>


namespace DB
{

namespace
{

enum class Kind
{
    Current,
    All
};

/** If ClickHouse is build with coverage instrumentation, returns an array
  * of currently accumulated (`coverage`) / all possible (`coverageAll`) unique code addresses.
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
        auto coverage_table = kind == Kind::Current ? getCoverage() : getAllInstrumentedAddresses();

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
    factory.registerFunction("coverage", [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionCoverage>(Kind::Current)); });
    factory.registerFunction("coverageAll", [](ContextPtr){ return std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionCoverage>(Kind::All)); });
}

}

#endif
