#pragma once

#include <base/scope_guard.h>
#include <boost/noncopyable.hpp>
#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_set>

namespace DB
{

/// Arbitrates temporary part directory names between active operations (merge/mutation/INSERT, via
/// `add`) and the background cleaner (via `tryHoldForCleanup`). A name is owned by at most one side:
/// the cleaner skips claimed names, and `add` waits out a cleanup in progress.
class TemporaryParts : private boost::noncopyable
{
private:
    /// To add const qualifier for contains()
    mutable std::mutex mutex;

    /// Signalled when a cleanup hold is released, so a pending `add` for the same name can proceed.
    std::condition_variable cleanup_finished;

    /// NOTE: It is pretty short, so use STL is fine.
    std::unordered_set<std::string> parts;

    /// Names the background cleaner is currently deleting; disjoint from `parts`.
    std::unordered_set<std::string> being_cleaned;

    void add(const std::string & basename);
    void remove(const std::string & basename);

    /// Takes a transient cleanup hold on the name. Returns an empty guard if an operation owns the name
    /// or another hold is already in place; otherwise the returned guard releases the hold, on every
    /// path including a failed removal, so a name whose removal threw stays retryable.
    scope_guard tryHoldForCleanup(const std::string & basename);

    friend class MergeTreeData;
    friend class TemporaryPartsTestAccessor;
public:
    /// Returns true if passed part name is active.
    /// (is the destination for one of active mutation/merge).
    ///
    /// NOTE: that it accept basename (i.e. dirname), not the path,
    /// since later requires canonical form.
    bool contains(const std::string & basename) const;
};

}
