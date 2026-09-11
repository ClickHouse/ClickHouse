#pragma once

#include <Common/Logger.h>

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>

namespace DB
{

/** Repeats a stage of an `undo` until object storage accepts it.
  *
  * A commit that fails changes nothing: the snapshot of the filesystem is published only after every operation has
  * succeeded, and the `undo` of the operations that did run restores what object storage held before. An `undo` that
  * stops halfway is different. The writes it already made stay in object storage, so object storage describes a
  * filesystem this process does not have, and nothing reconciles the two until the filesystem is rebuilt from object
  * storage. For a `MergeTree` part rename that means a part directory that turns up under a name the server never
  * committed.
  *
  * At that point the state of object storage is unknown, so no decision taken from it can be trusted, including the
  * decision to accept it. Therefore an `undo` does not give up: every stage repeats until it succeeds, and the
  * transaction reports its failure only once object storage agrees with the in-memory filesystem again. A stage that
  * succeeded never repeats, so an `undo` that restores several objects does not redo the ones it is done with.
  *
  * The cost is that a stage which keeps failing holds the thread, and with it every other transaction of the disk,
  * because a commit holds the metadata lock. This is deliberate. A disk whose metadata cannot be repaired must not
  * accept more metadata.
  *
  * A shutdown is the only exit. The retries stop, the transaction reports the failure, and the next start loads the
  * filesystem from object storage.
  *
  * A stage that throws `LOGICAL_ERROR` is not repeated, because no invariant is repaired by asking again. A stage uses
  * it to report the one state a reversal cannot leave: the blob it has to restore exists nowhere.
  */
class UndoWithRetries
{
public:
    /// Runs `stage` until it succeeds. Rethrows the last exception when a shutdown stops the retries.
    void runStage(const LoggerPtr & log, std::string_view description, const std::function<void()> & stage);

    /// Stops the retries of every `undo` in flight.
    void shutdown();

private:
    std::mutex mutex;
    std::condition_variable shutdown_condition;
    bool shutdown_called = false;
};

using UndoWithRetriesPtr = std::shared_ptr<UndoWithRetries>;

}
