#include <Interpreters/MergeTreeTransaction.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

MergeTreeTransaction::MergeTreeTransaction(Snapshot snapshot_, LocalTID local_tid_, UUID host_id)
    : tid({snapshot_, local_tid_, host_id})
    , snapshot(snapshot_)
    , state(RUNNING)
{
}

void MergeTreeTransaction::addNewPart(const DataPartPtr & new_part, MergeTreeTransaction * txn)
{
    TransactionID tid = txn ? txn->tid : Tx::PrehistoricTID;

    new_part->versions.setMinTID(tid);
    if (txn)
        txn->addNewPart(new_part);
}

void MergeTreeTransaction::removeOldPart(const DataPartPtr & part_to_remove, MergeTreeTransaction * txn)
{
    TransactionID tid = txn ? txn->tid : Tx::PrehistoricTID;
    String error_context = fmt::format("Table: {}, part name: {}",
                                       part_to_remove->storage.getStorageID().getNameForLogs(),
                                       part_to_remove->name);
    part_to_remove->versions.lockMaxTID(tid, error_context);
    if (txn)
        txn->removeOldPart(part_to_remove);
}

void MergeTreeTransaction::addNewPartAndRemoveCovered(const DataPartPtr & new_part, const DataPartsVector & covered_parts, MergeTreeTransaction * txn)
{
    TransactionID tid = txn ? txn->tid : Tx::PrehistoricTID;

    new_part->versions.setMinTID(tid);
    if (txn)
        txn->addNewPart(new_part);

    String error_context = fmt::format("Table: {}, covering part name: {}",
                                       new_part->storage.getStorageID().getNameForLogs(),
                                       new_part->name);
    error_context += ", part_name: {}";
    for (auto covered : covered_parts)
    {
        covered->versions.lockMaxTID(tid, fmt::format(error_context, covered->name));
        if (txn)
            txn->removeOldPart(covered);
    }
}

void MergeTreeTransaction::addNewPart(const DataPartPtr & new_part)
{
    creating_parts.push_back(new_part);
}

void MergeTreeTransaction::removeOldPart(const DataPartPtr & part_to_remove)
{
    removing_parts.push_back(part_to_remove);
}

bool MergeTreeTransaction::isReadOnly() const
{
    return creating_parts.empty() && removing_parts.empty();
}

void MergeTreeTransaction::beforeCommit()
{
    assert(state == RUNNING);
}

void MergeTreeTransaction::afterCommit()
{
    assert(state == COMMITTED);
    for (const auto & part : creating_parts)
        part->versions.mincsn.store(csn);
    for (const auto & part : removing_parts)
        part->versions.maxcsn.store(csn);
}

void MergeTreeTransaction::rollback()
{
    assert(state == RUNNING);
    for (const auto & part : creating_parts)
        part->versions.mincsn.store(Tx::RolledBackCSN);
    for (const auto & part : removing_parts)
        part->versions.unlockMaxTID(tid);
}

}
