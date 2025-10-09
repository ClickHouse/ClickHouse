#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <base/getThreadId.h>

namespace DB
{
namespace
{
    class FunctionTid : public IFunction
    {
    public:
        static constexpr auto name = "tid";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTid>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt64>(); }

        DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
        {
            return std::make_shared<DataTypeUInt64>();
        }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
        {
            auto current_tid = getThreadId();
            return DataTypeUInt64().createColumnConst(input_rows_count, current_tid);
        }
    };

}

REGISTER_FUNCTION(Tid)
{
    factory.registerFunction<FunctionTid>();
}

}
