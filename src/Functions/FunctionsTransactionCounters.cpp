#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/MergeTreeTransaction.h>


namespace DB
{

namespace
{

class FunctionTransactionID : public IFunction
{
public:
    static constexpr auto name = "transactionID";

    static FunctionPtr create(ContextConstPtr context)
    {
        return std::make_shared<FunctionTransactionID>(context->getCurrentTransaction());
    }

    explicit FunctionTransactionID(MergeTreeTransactionPtr && txn_) : txn(txn_)
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return TransactionID::getDataType();
    }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        Tuple res;
        if (txn)
            res = {txn->tid.start_csn, txn->tid.local_tid, txn->tid.host_id};
        else
            res = {UInt64(0), UInt64(0), UUIDHelpers::Nil};
        return result_type->createColumnConst(input_rows_count, res);
    }

private:
    MergeTreeTransactionPtr txn;
};

}

void registerFunctionsTransactionCounters(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTransactionID>();
}

}
