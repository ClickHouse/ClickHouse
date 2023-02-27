#include <Storages/System/StorageSystemTransactions.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/Context.h>


namespace DB
{

static DataTypePtr getStateEnumType()
{
    return std::make_shared<DataTypeEnum8>(
    DataTypeEnum8::Values
        {
            {"RUNNING",           static_cast<Int8>(MergeTreeTransaction::State::RUNNING)},
            {"COMMITTING",           static_cast<Int8>(MergeTreeTransaction::State::COMMITTING)},
            {"COMMITTED",         static_cast<Int8>(MergeTreeTransaction::State::COMMITTED)},
            {"ROLLED_BACK",       static_cast<Int8>(MergeTreeTransaction::State::ROLLED_BACK)},
        });
}

NamesAndTypesList StorageSystemTransactions::getNamesAndTypes()
{
    return {
        {"tid", getTransactionIDDataType()},
        {"tid_hash", std::make_shared<DataTypeUInt64>()},
        {"elapsed", std::make_shared<DataTypeFloat64>()},
        {"is_readonly", std::make_shared<DataTypeUInt8>()},
        {"state", getStateEnumType()},
    };
}

void StorageSystemTransactions::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    auto list = TransactionLog::instance().getTransactionsList();
    for (const auto & elem : list)
    {
        auto txn = elem.second;
        size_t i = 0;
        res_columns[i++]->insert(Tuple{txn->tid.start_csn, txn->tid.local_tid, txn->tid.host_id});
        res_columns[i++]->insert(txn->tid.getHash());
        res_columns[i++]->insert(txn->elapsedSeconds());
        res_columns[i++]->insert(txn->isReadOnly());
        res_columns[i++]->insert(txn->getState());
    }
}

}
