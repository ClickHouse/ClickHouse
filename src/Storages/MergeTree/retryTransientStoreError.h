#pragma once

#include <Common/Logger.h>
#include <Common/Stopwatch.h>

#include <functional>
#include <string_view>

namespace DB
{

/// Marks the scope of the `noexcept` commit and rollback callbacks of `MergeTreeTransaction`,
/// whose metadata writes have no caller to report an error to. The scope carries the retry
/// budget of `retryTransientStoreError`, 60 seconds shared by all the writes made inside it.
class TransientStoreRetryScope
{
public:
    TransientStoreRetryScope();
    ~TransientStoreRetryScope();

    TransientStoreRetryScope(const TransientStoreRetryScope &) = delete;
    TransientStoreRetryScope & operator=(const TransientStoreRetryScope &) = delete;

    /// The innermost scope of this thread, or `nullptr`.
    static const TransientStoreRetryScope * current();
    double elapsedSeconds() const { return watch.elapsedSeconds(); }

private:
    Stopwatch watch;
    const TransientStoreRetryScope * outer;
};

/// Runs `store`, a write of MergeTree metadata (`txn_version.txt`, `mutation_N.txt`), again
/// after a transient storage error, with backoff, inside a `TransientStoreRetryScope` while
/// its budget lasts. Transient is decided by `isRetryableException`, plus the errnos of a
/// failed local write (`ENOSPC`, `EROFS`, `EIO`, ...), minus memory limits. Any other error
/// is rethrown at once, as is the last error when the budget is exhausted or the server is
/// shutting down, so a write that did not land is never hidden. Outside a scope the first
/// error is rethrown: the write is on a query path, and the client gets the error.
void retryTransientStoreError(LoggerPtr log, std::string_view what, const std::function<void()> & store);

}
