#include <Functions/FunctionConstantBase.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/TransactionLog.h>


namespace DB
{

namespace
{

class FunctionTransactionID : public FunctionConstantBase<FunctionTransactionID, Tuple, DataTypeNothing>
{
public:
    static constexpr auto name = "transactionID";
    static Tuple getValue(const MergeTreeTransactionPtr & txn)
    {
        Tuple res;
        if (txn)
            res = {txn->tid.start_csn, txn->tid.local_tid, txn->tid.host_id};
        else
            res = {static_cast<UInt64>(0), static_cast<UInt64>(0), UUIDHelpers::Nil};
        return res;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return getTransactionIDDataType(); }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTransactionID>(context); }
    explicit FunctionTransactionID(ContextPtr context) : FunctionConstantBase(getValue(context->getCurrentTransaction()), context->isDistributed()) {}
};

class FunctionTransactionLatestSnapshot : public FunctionConstantBase<FunctionTransactionLatestSnapshot, UInt64, DataTypeUInt64>
{
    static UInt64 getLatestSnapshot(ContextPtr context)
    {
        context->checkTransactionsAreAllowed(/* explicit_tcl_query */ true);
        return TransactionLog::instance().getLatestSnapshot();
    }
public:
    static constexpr auto name = "transactionLatestSnapshot";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTransactionLatestSnapshot>(context); }
    explicit FunctionTransactionLatestSnapshot(ContextPtr context) : FunctionConstantBase(getLatestSnapshot(context), context->isDistributed()) {}
};

class FunctionTransactionOldestSnapshot : public FunctionConstantBase<FunctionTransactionOldestSnapshot, UInt64, DataTypeUInt64>
{
    static UInt64 getOldestSnapshot(ContextPtr context)
    {
        context->checkTransactionsAreAllowed(/* explicit_tcl_query */ true);
        return TransactionLog::instance().getOldestSnapshot();
    }
public:
    static constexpr auto name = "transactionOldestSnapshot";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTransactionOldestSnapshot>(context); }
    explicit FunctionTransactionOldestSnapshot(ContextPtr context) : FunctionConstantBase(getOldestSnapshot(context), context->isDistributed()) {}
};

}

void registerFunctionsTransactionCounters(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTransactionID>();
    factory.registerFunction<FunctionTransactionLatestSnapshot>();
    factory.registerFunction<FunctionTransactionOldestSnapshot>();
}

}
