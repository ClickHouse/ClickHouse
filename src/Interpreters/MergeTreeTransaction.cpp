#include <Interpreters/MergeTreeTransaction.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/TransactionLog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_TRANSACTION;
    extern const int LOGICAL_ERROR;
}

MergeTreeTransaction::MergeTreeTransaction(Snapshot snapshot_, LocalTID local_tid_, UUID host_id)
    : tid({snapshot_, local_tid_, host_id})
    , snapshot(snapshot_)
    , csn(Tx::UnknownCSN)
{
}

MergeTreeTransaction::State MergeTreeTransaction::getState() const
{
    CSN c = csn.load();
    if (c == Tx::UnknownCSN || c == Tx::CommittingCSN)
        return RUNNING;
    if (c == Tx::RolledBackCSN)
        return ROLLED_BACK;
    return COMMITTED;
}

void MergeTreeTransaction::addNewPart(const StoragePtr & storage, const DataPartPtr & new_part, MergeTreeTransaction * txn)
{
    TransactionID tid = txn ? txn->tid : Tx::PrehistoricTID;

    new_part->versions.setMinTID(tid, TransactionInfoContext{storage->getStorageID(), new_part->name});
    if (txn)
        txn->addNewPart(storage, new_part);
}

void MergeTreeTransaction::removeOldPart(const StoragePtr & storage, const DataPartPtr & part_to_remove, MergeTreeTransaction * txn)
{
    TransactionID tid = txn ? txn->tid : Tx::PrehistoricTID;
    TransactionInfoContext context{storage->getStorageID(), part_to_remove->name};
    part_to_remove->versions.lockMaxTID(tid, context);
    if (txn)
        txn->removeOldPart(storage, part_to_remove);
}

void MergeTreeTransaction::addNewPartAndRemoveCovered(const StoragePtr & storage, const DataPartPtr & new_part, const DataPartsVector & covered_parts, MergeTreeTransaction * txn)
{
    TransactionID tid = txn ? txn->tid : Tx::PrehistoricTID;

    TransactionInfoContext context{storage->getStorageID(), new_part->name};
    new_part->versions.setMinTID(tid, context);
    if (txn)
        txn->addNewPart(storage, new_part);

    context.covering_part = std::move(context.part_name);
    for (const auto & covered : covered_parts)
    {
        context.part_name = covered->name;
        covered->versions.lockMaxTID(tid, context);
        if (txn)
            txn->removeOldPart(storage, covered);
    }
}

void MergeTreeTransaction::addNewPart(const StoragePtr & storage, const DataPartPtr & new_part)
{
    CSN c = csn.load();
    if (c == Tx::RolledBackCSN)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction was cancelled");
    else if (c != Tx::UnknownCSN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CSN state: {}", c);

    storages.insert(storage);
    creating_parts.push_back(new_part);
    new_part->storeVersionMetadata();
}

void MergeTreeTransaction::removeOldPart(const StoragePtr & storage, const DataPartPtr & part_to_remove)
{
    CSN c = csn.load();
    if (c == Tx::RolledBackCSN)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction was cancelled");//FIXME
    else if (c != Tx::UnknownCSN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CSN state: {}", c);

    storages.insert(storage);
    removing_parts.push_back(part_to_remove);
    part_to_remove->storeVersionMetadata();
}

void MergeTreeTransaction::addMutation(const StoragePtr & table, const String & mutation_id)
{
    mutations.emplace_back(table, mutation_id);
}

bool MergeTreeTransaction::isReadOnly() const
{
    return creating_parts.empty() && removing_parts.empty();
}

void MergeTreeTransaction::beforeCommit()
{
    for (const auto & table_and_mutation : mutations)
        table_and_mutation.first->waitForMutation(table_and_mutation.second);

    CSN expected = Tx::UnknownCSN;
    bool can_commit = csn.compare_exchange_strong(expected, Tx::CommittingCSN);
    if (can_commit)
        return;

    if (expected == Tx::RolledBackCSN)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction was cancelled");
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CSN state: {}", expected);
}

void MergeTreeTransaction::afterCommit(CSN assigned_csn) noexcept
{
    [[maybe_unused]] CSN prev_value = csn.exchange(assigned_csn);
    assert(prev_value == Tx::CommittingCSN);
    for (const auto & part : creating_parts)
    {
        part->versions.mincsn.store(csn);
        part->storeVersionMetadata();
    }

    for (const auto & part : removing_parts)
    {
        part->versions.maxcsn.store(csn);
        part->storeVersionMetadata();
    }
}

bool MergeTreeTransaction::rollback() noexcept
{
    CSN expected = Tx::UnknownCSN;
    bool need_rollback = csn.compare_exchange_strong(expected, Tx::RolledBackCSN);

    if (!need_rollback)
        return false;

    for (const auto & table_and_mutation : mutations)
        table_and_mutation.first->killMutation(table_and_mutation.second);

    for (const auto & part : creating_parts)
        part->versions.mincsn.store(Tx::RolledBackCSN);

    for (const auto & part : removing_parts)
        part->versions.unlockMaxTID(tid, TransactionInfoContext{part->storage.getStorageID(), part->name});

    /// FIXME const_cast
    for (const auto & part : creating_parts)
        const_cast<MergeTreeData &>(part->storage).removePartsFromWorkingSet(nullptr, {part}, true);

    for (const auto & part : removing_parts)
        if (part->versions.getMinTID() != tid)
            const_cast<MergeTreeData &>(part->storage).restoreAndActivatePart(part);

    return true;
}

void MergeTreeTransaction::onException()
{
    TransactionLog::instance().rollbackTransaction(shared_from_this());
}

String MergeTreeTransaction::dumpDescription() const
{
    String res = "\ncreating parts:\n";
    for (const auto & part : creating_parts)
    {
        res += part->name;
        res += "\n";
    }

    res += "removing parts:\n";
    for (const auto & part : removing_parts)
    {
        res += part->name;
        res += fmt::format(" (created by {}, {})\n", part->versions.getMinTID(), part->versions.mincsn);
        assert(!part->versions.mincsn || part->versions.mincsn <= snapshot);
        assert(!part->versions.maxcsn);
    }

    return res;
}

}
