#pragma once
#include <memory>

namespace DB
{

class MergeTreeTransaction;
using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

class MergeTreeTransactionHolder
{
public:
    MergeTreeTransactionHolder() = default;
    MergeTreeTransactionHolder(const MergeTreeTransactionPtr & txn_, bool autocommit_);
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
};

}
