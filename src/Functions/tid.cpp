#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/castTypeToEither.h>

#include <thread>

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

        void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t) const override
        {
            UInt64 current_tid = std::hash<std::thread::id>{}(std::this_thread::get_id());

            block.getByPosition(result).column = DataTypeUInt64().createColumnConst(1, current_tid);
        }
    };

}

void registerFunctionTid(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTid>();
}

}
