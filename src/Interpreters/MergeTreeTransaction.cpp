#include <Interpreters/MergeTreeTransaction.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Common/noexcept_scope.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_TRANSACTION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

static void checkNotOrdinaryDatabase(const StoragePtr & storage)
{
    if (storage->getStorageID().uuid != UUIDHelpers::Nil)
        return;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table {} belongs to database with Ordinary engine. "
                    "This engine is deprecated and is not supported in transactions.", storage->getStorageID().getNameForLogs());
}

MergeTreeTransaction::MergeTreeTransaction(CSN snapshot_, LocalTID local_tid_, UUID host_id, std::list<CSN>::iterator snapshot_it_)
    : tid({snapshot_, local_tid_, host_id})
    , snapshot(snapshot_)
    , snapshot_in_use_it(snapshot_it_)
    , csn(Tx::UnknownCSN)
{
}

void MergeTreeTransaction::setSnapshot(CSN new_snapshot)
{
    snapshot.store(new_snapshot, std::memory_order_relaxed);
}

MergeTreeTransaction::State MergeTreeTransaction::getState() const
{
    CSN c = csn.load();
    if (c == Tx::UnknownCSN)
        return RUNNING;
    if (c == Tx::CommittingCSN)
        return COMMITTING;
    if (c == Tx::RolledBackCSN)
        return ROLLED_BACK;
    return COMMITTED;
}

bool MergeTreeTransaction::waitStateChange(CSN current_state_csn) const
{
    CSN current_value = current_state_csn;
    while (current_value == current_state_csn && !TransactionLog::instance().isShuttingDown())
    {
        csn.wait(current_value);
        current_value = csn.load();
    }
    return current_value != current_state_csn;
}

void MergeTreeTransaction::checkIsNotCancelled() const
{
    CSN c = csn.load();
    if (c == Tx::RolledBackCSN)
        throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction was cancelled");
    else if (c != Tx::UnknownCSN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CSN state: {}", c);
}

void MergeTreeTransaction::addNewPart(const StoragePtr & storage, const DataPartPtr & new_part, MergeTreeTransaction * txn)
{
    /// Creation TID was written to data part earlier on part creation.
    /// We only need to ensure that it's written and add part to in-memory set of new parts.
    new_part->assertHasVersionMetadata(txn);
    if (txn)
    {
        txn->addNewPart(storage, new_part);
        /// Now we know actual part name and can write it to system log table.
        tryWriteEventToSystemLog(new_part->version.log, TransactionsInfoLogElement::ADD_PART, txn->tid, TransactionInfoContext{storage->getStorageID(), new_part->name});
    }
}

void MergeTreeTransaction::removeOldPart(const StoragePtr & storage, const DataPartPtr & part_to_remove, MergeTreeTransaction * txn)
{
    TransactionInfoContext transaction_context{storage->getStorageID(), part_to_remove->name};
    if (txn)
    {
        /// Lock part for removal and write current TID into version metadata file.
        /// If server crash just after committing transactions
        /// we will find this TID in version metadata and will finally remove part.
        txn->removeOldPart(storage, part_to_remove, transaction_context);
    }
    else
    {
        /// Lock part for removal with special TID, so transactions will not try to remove it concurrently.
        /// We lock it only in memory if part was not involved in any transactions.
        part_to_remove->version.lockRemovalTID(Tx::PrehistoricTID, transaction_context);
        if (part_to_remove->wasInvolvedInTransaction())
            part_to_remove->appendRemovalTIDToVersionMetadata();
    }
}

void MergeTreeTransaction::addNewPartAndRemoveCovered(const StoragePtr & storage, const DataPartPtr & new_part, const DataPartsVector & covered_parts, MergeTreeTransaction * txn)
{
    TransactionID tid = txn ? txn->tid : Tx::PrehistoricTID;
    TransactionInfoContext transaction_context{storage->getStorageID(), new_part->name};
    tryWriteEventToSystemLog(new_part->version.log, TransactionsInfoLogElement::ADD_PART, tid, transaction_context);
    transaction_context.covering_part = std::move(transaction_context.part_name);
    new_part->assertHasVersionMetadata(txn);

    if (txn)
    {
        txn->addNewPart(storage, new_part);
        for (const auto & covered : covered_parts)
        {
            transaction_context.part_name = covered->name;
            txn->removeOldPart(storage, covered, transaction_context);
        }
    }
    else
    {
        for (const auto & covered : covered_parts)
        {
            transaction_context.part_name = covered->name;
            covered->version.lockRemovalTID(tid, transaction_context);
        }
    }
}

void MergeTreeTransaction::addNewPart(const StoragePtr & storage, const DataPartPtr & new_part)
{
    checkNotOrdinaryDatabase(storage);
    std::lock_guard lock{mutex};
    checkIsNotCancelled();
    storages.insert(storage);
    creating_parts.push_back(new_part);
}

void MergeTreeTransaction::removeOldPart(const StoragePtr & storage, const DataPartPtr & part_to_remove, const TransactionInfoContext & context)
{
    checkNotOrdinaryDatabase(storage);

    {
        std::lock_guard lock{mutex};
        checkIsNotCancelled();

        part_to_remove->version.lockRemovalTID(tid, context);
        NOEXCEPT_SCOPE({
            storages.insert(storage);
            removing_parts.push_back(part_to_remove);
        });
    }

    part_to_remove->appendRemovalTIDToVersionMetadata();
}

void MergeTreeTransaction::addMutation(const StoragePtr & table, const String & mutation_id)
{
    checkNotOrdinaryDatabase(table);
    std::lock_guard lock{mutex};
    checkIsNotCancelled();
    storages.insert(table);
    mutations.emplace_back(table, mutation_id);
}

bool MergeTreeTransaction::isReadOnly() const
{
    std::lock_guard lock{mutex};
    chassert((creating_parts.empty() && removing_parts.empty() && mutations.empty()) == storages.empty());
    return storages.empty();
}

scope_guard MergeTreeTransaction::beforeCommit()
{
    RunningMutationsList mutations_to_wait;
    {
        std::lock_guard lock{mutex};
        mutations_to_wait = mutations;
    }

    /// We should wait for mutations to finish before committing transaction, because some mutation may fail and cause rollback.
    for (const auto & table_and_mutation : mutations_to_wait)
        table_and_mutation.first->waitForMutation(table_and_mutation.second);

    assert([&]()
    {
        std::lock_guard lock{mutex};
        return mutations == mutations_to_wait;
    }());

    CSN expected = Tx::UnknownCSN;
    bool can_commit = csn.compare_exchange_strong(expected, Tx::CommittingCSN);
    if (!can_commit)
    {
        /// Transaction was concurrently cancelled by KILL TRANSACTION or KILL MUTATION
        if (expected == Tx::RolledBackCSN)
            throw Exception(ErrorCodes::INVALID_TRANSACTION, "Transaction was cancelled");
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CSN state: {}", expected);
    }

    /// We should set CSN back to Unknown if we will fail to commit transaction for some reason (connection loss, etc)
    return [this]()
    {
        CSN expected_value = Tx::CommittingCSN;
        csn.compare_exchange_strong(expected_value, Tx::UnknownCSN);
    };
}

void MergeTreeTransaction::afterCommit(CSN assigned_csn) noexcept
{
    /// Write allocated CSN into version metadata, so we will know CSN without reading it from transaction log
    /// and we will be able to remove old entries from transaction log in ZK.
    /// It's not a problem if server crash before CSN is written, because we already have TID in data part and entry in the log.
    [[maybe_unused]] CSN prev_value = csn.exchange(assigned_csn);
    chassert(prev_value == Tx::CommittingCSN);

    DataPartsVector created_parts;
    DataPartsVector removed_parts;
    RunningMutationsList committed_mutations;
    {
        /// We don't really need mutex here, because no concurrent modifications of transaction object may happen after commit.
        std::lock_guard lock{mutex};
        created_parts = creating_parts;
        removed_parts = removing_parts;
        committed_mutations = mutations;
    }

    for (const auto & part : created_parts)
    {
        part->version.creation_csn.store(csn);
        part->appendCSNToVersionMetadata(VersionMetadata::WhichCSN::CREATION);
    }

    for (const auto & part : removed_parts)
    {
        part->version.removal_csn.store(csn);
        part->appendCSNToVersionMetadata(VersionMetadata::WhichCSN::REMOVAL);
    }

    for (const auto & storage_and_mutation : committed_mutations)
        storage_and_mutation.first->setMutationCSN(storage_and_mutation.second, csn);
}

bool MergeTreeTransaction::rollback() noexcept
{
    CSN expected = Tx::UnknownCSN;
    bool need_rollback = csn.compare_exchange_strong(expected, Tx::RolledBackCSN);

    /// Check that it was not rolled back concurrently
    if (!need_rollback)
        return false;

    /// It's not a problem if server crash at this point
    /// because on startup we will see that TID is not committed and will simply discard these changes.

    RunningMutationsList mutations_to_kill;
    DataPartsVector parts_to_remove;
    DataPartsVector parts_to_activate;

    {
        std::lock_guard lock{mutex};
        mutations_to_kill = mutations;
        parts_to_remove = creating_parts;
        parts_to_activate = removing_parts;
    }

    /// Forcefully stop related mutations if any
    for (const auto & table_and_mutation : mutations_to_kill)
        table_and_mutation.first->killMutation(table_and_mutation.second);

    /// Discard changes in active parts set
    /// Remove parts that were created, restore parts that were removed (except parts that were created by this transaction too)

    /// Kind of optimization: cleanup thread can remove these parts immediately
    for (const auto & part : parts_to_remove)
    {
        part->version.creation_csn.store(Tx::RolledBackCSN);
        /// Write special RolledBackCSN, so we will be able to cleanup transaction log
        part->appendCSNToVersionMetadata(VersionMetadata::CREATION);
    }

    for (const auto & part : parts_to_remove)
    {
        /// NOTE It's possible that part is already removed from working set in the same transaction
        /// (or, even worse, in a separate non-transactional query with PrehistoricTID),
        /// but it's not a problem: removePartsFromWorkingSet(...) will do nothing in this case.
        const_cast<MergeTreeData &>(part->storage).removePartsFromWorkingSet(NO_TRANSACTION_RAW, {part}, true);
    }

    for (const auto & part : parts_to_activate)
        if (part->version.getCreationTID() != tid)
            const_cast<MergeTreeData &>(part->storage).restoreAndActivatePart(part);

    for (const auto & part : parts_to_activate)
    {
        /// Clear removal_tid from version metadata file, so we will not need to distinguish TIDs that were not committed
        /// and TIDs that were committed long time ago and were removed from the log on log cleanup.
        part->appendRemovalTIDToVersionMetadata(/* clear */ true);
        part->version.unlockRemovalTID(tid, TransactionInfoContext{part->storage.getStorageID(), part->name});
    }


    assert([&]()
    {
        std::lock_guard lock{mutex};
        assert(mutations_to_kill == mutations);
        assert(parts_to_remove == creating_parts);
        assert(parts_to_activate == removing_parts);
        return csn == Tx::RolledBackCSN;
    }());

    return true;
}

void MergeTreeTransaction::onException()
{
    TransactionLog::instance().rollbackTransaction(shared_from_this());
}

String MergeTreeTransaction::dumpDescription() const
{
    String res = fmt::format("{} state: {}, snapshot: {}", tid, getState(), getSnapshot());

    if (isReadOnly())
    {
        res += ", readonly";
        return res;
    }

    std::lock_guard lock{mutex};

    res += fmt::format(", affects {} tables:", storages.size());

    using ChangesInTable = std::tuple<Strings, Strings, Strings>;
    std::unordered_map<const IStorage *, ChangesInTable> storage_to_changes;

    for (const auto & part : creating_parts)
        std::get<0>(storage_to_changes[&(part->storage)]).push_back(part->name);

    for (const auto & part : removing_parts)
    {
        String info = fmt::format("{} (created by {}, {})", part->name, part->version.getCreationTID(), part->version.creation_csn);
        std::get<1>(storage_to_changes[&(part->storage)]).push_back(std::move(info));
        chassert(!part->version.creation_csn || part->version.creation_csn <= getSnapshot());
    }

    for (const auto & mutation : mutations)
        std::get<2>(storage_to_changes[mutation.first.get()]).push_back(mutation.second);

    for (const auto & storage_changes : storage_to_changes)
    {
        res += fmt::format("\n\t{}:", storage_changes.first->getStorageID().getNameForLogs());
        const auto & creating_info = std::get<0>(storage_changes.second);
        const auto & removing_info = std::get<1>(storage_changes.second);
        const auto & mutations_info = std::get<2>(storage_changes.second);

        if (!creating_info.empty())
            res += fmt::format("\n\t\tcreating parts:\n\t\t\t{}", fmt::join(creating_info, "\n\t\t\t"));
        if (!removing_info.empty())
            res += fmt::format("\n\t\tremoving parts:\n\t\t\t{}", fmt::join(removing_info, "\n\t\t\t"));
        if (!mutations_info.empty())
            res += fmt::format("\n\t\tmutations:\n\t\t\t{}", fmt::join(mutations_info, "\n\t\t\t"));
    }

    return res;
}

}
