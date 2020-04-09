#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Access/QuotaContext.h>
#include <Core/Field.h>


namespace DB
{

class FunctionCurrentQuota : public IFunction
{
    const String quota_name;

public:
    static constexpr auto name = "currentQuota";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionCurrentQuota>(context.getQuota()->getUsageInfo().quota_name);
    }

    explicit FunctionCurrentQuota(const String & quota_name_) : quota_name{quota_name_}
    {
    }

    String getName() const override
    {
        return name;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, quota_name);
    }
};


class FunctionCurrentQuotaId : public IFunction
{
    const UUID quota_id;

public:
    static constexpr auto name = "currentQuotaID";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionCurrentQuotaId>(context.getQuota()->getUsageInfo().quota_id);
    }

    explicit FunctionCurrentQuotaId(const UUID quota_id_) : quota_id{quota_id_}
    {
    }

    String getName() const override
    {
        return name;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUUID>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeUUID().createColumnConst(input_rows_count, quota_id);
    }
};


class FunctionCurrentQuotaKey : public IFunction
{
    const String quota_key;

public:
    static constexpr auto name = "currentQuotaKey";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionCurrentQuotaKey>(context.getQuota()->getUsageInfo().quota_key);
    }

    explicit FunctionCurrentQuotaKey(const String & quota_key_) : quota_key{quota_key_}
    {
    }

    String getName() const override
    {
        return name;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, quota_key);
    }
};


void registerFunctionCurrentQuota(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCurrentQuota>();
    factory.registerFunction<FunctionCurrentQuotaId>();
    factory.registerFunction<FunctionCurrentQuotaKey>();
}

}
