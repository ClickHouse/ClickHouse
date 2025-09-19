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

REGISTER_FUNCTION(TransactionCounters)
{
    FunctionDocumentation::Description description_transactionID = R"(
<ExperimentalBadge/>
<CloudNotSupportedBadge/>

Returns the ID of a transaction.

:::note
This function is part of an experimental feature set.
Enable experimental transaction support by adding this setting to your [configuration](/operations/configuration-files):

```xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

For more information see the page [Transactional (ACID) support](/guides/developer/transactional#transactions-commit-and-rollback).
:::
    )";
    FunctionDocumentation::Syntax syntax_transactionID = "transactionID()";
    FunctionDocumentation::Arguments arguments_transactionID = {};
    FunctionDocumentation::ReturnedValue returned_value_transactionID = {
    R"(
Returns a tuple consisting of `start_csn`, `local_tid` and `host_id`.
- `start_csn`: Global sequential number, the newest commit timestamp that was seen when this transaction began.
- `local_tid`: Local sequential number that is unique for each transaction started by this host within a specific start_csn.
- `host_id`: UUID of the host that has started this transaction.
    )",
    {"Tuple(UInt64, UInt64, UUID)"}
    };
    FunctionDocumentation::Examples examples_transactionID = {
    {
        "Usage example",
        R"(
BEGIN TRANSACTION;
SELECT transactionID();
ROLLBACK;
        )",
        R"(
┌─transactionID()────────────────────────────────┐
│ (32,34,'0ee8b069-f2bb-4748-9eae-069c85b5252b') │
└────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_transactionID = {22, 6};
    FunctionDocumentation::Category category_transactionID = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_transactionID = {description_transactionID, syntax_transactionID, arguments_transactionID, {}, returned_value_transactionID, examples_transactionID, introduced_in_transactionID, category_transactionID};

    factory.registerFunction<FunctionTransactionID>(documentation_transactionID);
    factory.registerFunction<FunctionTransactionLatestSnapshot>();
    factory.registerFunction<FunctionTransactionOldestSnapshot>();
}

}
