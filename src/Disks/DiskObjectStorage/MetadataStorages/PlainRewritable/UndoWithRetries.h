#pragma once

#include <Common/Logger.h>

#include <functional>
#include <string_view>

namespace DB
{

/** Repeats a stage of an `undo` until object storage accepts it.
  *
  * An `undo` that stops halfway leaves its earlier writes behind while the failed transaction publishes nothing, so
  * object storage describes a filesystem this process does not have until the next load. What is stored is unknown
  * from here, so the reversal does not give up; a stage that succeeded never repeats. A stage that keeps failing holds
  * the thread, and the metadata lock with it, which is deliberate - a disk whose metadata cannot be repaired must not
  * accept more metadata.
  *
  * `LOGICAL_ERROR` is not repeated, because asking again repairs no invariant. A stage throws it when the blob it has
  * to restore exists nowhere; `MetadataOperationsHolder::rollback` then stops, and the operations below it keep their
  * writes.
  */
void undoWithRetries(const LoggerPtr & log, std::string_view description, const std::function<void()> & stage);

}
