#pragma once

#include <functional>
#include <string>
#include <vector>

namespace DB
{

class WriteBufferFromFileBase;
struct MergeTreeWriterStream;

/// Calls sync() on each file in parallel using the IO thread pool.
/// fsync of multiple files inside the same part is independent and safe to run concurrently.
/// If there is only one file, it is synced inline (no thread pool overhead).
void parallelSyncFiles(const std::vector<WriteBufferFromFileBase *> & files);

/// Same, but takes MergeTreeWriterStream pointers (each stream contains plain_file and marks_file
/// that are synced together).
void parallelSyncFiles(const std::vector<const MergeTreeWriterStream *> & streams);

/// Opens each file on the local filesystem by its path and calls fsync on it, in parallel.
/// Used to make durable the files that were already written and closed.
/// If the fsync of a file fails, `on_failure` is called with its index before the exception is rethrown.
void parallelSyncPaths(const std::vector<std::string> & paths, const std::function<void(size_t)> & on_failure);

}
