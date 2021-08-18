#include <Core/ServerUUID.h>
#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace
{

class FunctionServerUUID : public IFunction
    {
    public:
        static constexpr auto name = "serverUUID";

        static FunctionPtr create(ContextPtr context)
        {
            return std::make_shared<FunctionServerUUID>(context->isDistributed(), ServerUUID::get());
        }

        explicit FunctionServerUUID(bool is_distributed_, UUID server_uuid_)
            : is_distributed(is_distributed_), server_uuid(server_uuid_)
        {
        }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUUID>(); }

        bool isDeterministic() const override { return false; }

        bool isDeterministicInScopeOfQuery() const override { return true; }

        bool isSuitableForConstantFolding() const override { return !is_distributed; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
        {
            return DataTypeUUID().createColumnConst(input_rows_count, server_uuid);
        }

    private:
        bool is_distributed;
        const UUID server_uuid;
    };

}

void registerFunctionServerUUID(FunctionFactory & factory)
{
    factory.registerFunction<FunctionServerUUID>();
}

}

