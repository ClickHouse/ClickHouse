#pragma once

#include <Common/ThreadPool_fwd.h>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>

namespace DB::Cas
{

/// Server-wide pool for the parallel intra-part blob upload fan-out (stage-1 design §1,
/// "Parallel blob upload within a part"). Deliberately disjoint from
/// `IObjectStorage::getThreadPoolWriter`: an upload task may itself submit to the writer pool (S3
/// multipart), so nesting the fan-out on that same pool would risk the classic same-pool
/// wait-on-self deadlock. The calling thread only submits tasks and joins them -- it never
/// occupies a pool slot itself -- so pool size 1 is a valid (fully serial) configuration, never a
/// deadlock risk.
///
/// Fail-loud lifecycle: the server (or a test) must call `initializeBlobUploadPool` before any
/// `blobUploadPool` use. There is no lazy self-initialization on the production path.

/// Throws `BAD_ARGUMENTS` if `size == 0`. Throws `LOGICAL_ERROR` if already initialized.
void initializeBlobUploadPool(size_t size);

/// Throws `LOGICAL_ERROR` if the pool has not been initialized. The returned reference is only
/// valid while the pool stays initialized: callers must not race this against
/// `shutdownBlobUploadPool` (in the server, shutdown runs after query drain; tests own the order).
ThreadPool & blobUploadPool();

/// Idempotent: safe to call multiple times, and safe to call even if never initialized. Joins all
/// outstanding tasks before returning.
void shutdownBlobUploadPool() noexcept;

/// For tests only: true once `initializeBlobUploadPool` has run, false before that call and after
/// `shutdownBlobUploadPool`.
bool blobUploadPoolInitializedForTest();



}
