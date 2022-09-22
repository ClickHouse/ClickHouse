#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
namespace
{
    // This class is a stub for the meiliMatch function in the where section of the query,
    // this function is used to pass parameters to the MeiliSearch storage engine
    class FunctionMeiliMatch : public IFunction
    {
    public:
        static constexpr auto name = "meiliMatch";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMeiliMatch>(); }

        /// Get the function name.
        String getName() const override { return name; }

        bool isStateful() const override { return false; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        size_t getNumberOfArguments() const override { return 0; }

        bool isVariadic() const override { return true; }

        bool isDeterministic() const override { return false; }

        bool isDeterministicInScopeOfQuery() const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeUInt8>(); }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
        {
            return ColumnUInt8::create(input_rows_count, 1u);
        }
    };

}

REGISTER_FUNCTION(MeiliMatch)
{
    factory.registerFunction<FunctionMeiliMatch>();
}

}
