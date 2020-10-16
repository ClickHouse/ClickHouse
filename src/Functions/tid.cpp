#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>

#include <common/getThreadId.h>

namespace DB
{
namespace
{
    class FunctionTid : public IFunction
    {
    public:
        static constexpr auto name = "tid";
        static FunctionPtr create(const Context &) { return std::make_shared<FunctionTid>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt64>(); }

        void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers &, size_t result, size_t input_rows_count) const override
        {
            auto current_tid = getThreadId();
            columns[result].column = DataTypeUInt64().createColumnConst(input_rows_count, current_tid);
        }
    };

}

void registerFunctionTid(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTid>();
}

}
