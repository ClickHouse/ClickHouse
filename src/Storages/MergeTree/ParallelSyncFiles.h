#pragma once

#include <string>
#include <vector>

namespace DB
{

class IDisk;
class WriteBufferFromFileBase;
struct MergeTreeWriterStream;

/// Calls sync() on each file in parallel using the IO thread pool.
/// fsync of multiple files inside the same part is independent and safe to run concurrently.
/// If there is only one file, it is synced inline (no thread pool overhead).
void parallelSyncFiles(const std::vector<WriteBufferFromFileBase *> & files);

/// Same, but takes MergeTreeWriterStream pointers (each stream contains plain_file and marks_file
/// that are synced together).
void parallelSyncFiles(const std::vector<const MergeTreeWriterStream *> & streams);

/// Same, but syncs already written and closed files by their path on `disk`.
/// Used to fsync a whole set of finalized parts at once (see IDataPartStorage::syncFiles).
void parallelSyncFiles(const IDisk & disk, const std::vector<std::string> & paths);

}
