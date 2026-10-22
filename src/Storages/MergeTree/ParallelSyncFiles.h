#pragma once

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

}
