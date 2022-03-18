#pragma once
#include <memory>

namespace DB
{

class Context;

class MergeTreeTransaction;
/// TODO maybe replace with raw pointer? It should not be shared, only MergeTreeTransactionHolder can own a transaction object
using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

/// Owns a MergeTreeTransactionObject.
/// Rolls back a transaction in dtor if it was not committed.
/// If `autocommit` flag is true, then it commits transaction if dtor is called normally
/// or rolls it back if dtor was called due to an exception.
class MergeTreeTransactionHolder
{
public:
    MergeTreeTransactionHolder() = default;
    MergeTreeTransactionHolder(const MergeTreeTransactionPtr & txn_, bool autocommit_, const Context * owned_by_session_context_ = nullptr);
    MergeTreeTransactionHolder(MergeTreeTransactionHolder && rhs) noexcept;
    MergeTreeTransactionHolder & operator=(MergeTreeTransactionHolder && rhs) noexcept;
    ~MergeTreeTransactionHolder();

    /// NOTE: We cannot make it noncopyable, because we use it as a field of Context.
    /// So the following copy constructor and operator does not copy anything,
    /// they just leave txn nullptr.
    MergeTreeTransactionHolder(const MergeTreeTransactionHolder & rhs);
    MergeTreeTransactionHolder & operator=(const MergeTreeTransactionHolder & rhs);

    MergeTreeTransactionPtr getTransaction() const { return txn; }

private:
    void onDestroy() noexcept;

    MergeTreeTransactionPtr txn;
    bool autocommit = false;
    const Context * owned_by_session_context = nullptr;
};

}
