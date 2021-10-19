#pragma once

#include <IO/Progress.h>
#include <Interpreters/Context.h>
#include <Common/Stopwatch.h>


/// http://en.wikipedia.org/wiki/ANSI_escape_code
#define CLEAR_TO_END_OF_LINE "\033[K"

namespace DB
{

class ProgressIndication
{
public:
    /// Write progress to stderr.
    void writeProgress();

    void writeFinalProgress();

    /// Clear stderr output.
    void clearProgressOutput();

    /// Reset progress values.
    void resetProgress();

    /// Update Progress object. It can be updated from:
    /// 1. onProgress in clickhouse-client;
    /// 2. ProgressCallback via setProgressCallback methrod in:
    ///    - context (used in clickhouse-local, can also be added in arbitrary place)
    ///    - SourceWithProgress (also in streams)
    ///    - readBufferFromFileDescriptor (for file processing progress)
    bool updateProgress(const Progress & value);

    /// In some cases there is a need to update progress value, when there is no access to progress_inidcation object.
    /// In this case it is added via context.
    /// `write_progress_on_update` is needed to write progress for loading files data via pipe in non-interactive mode.
    void setFileProgressCallback(ContextMutablePtr context, bool write_progress_on_update = false);

    /// How much seconds passed since query execution start.
    double elapsedSeconds() const { return watch.elapsedSeconds(); }

private:
    /// This flag controls whether to show the progress bar. We start showing it after
    /// the query has been executing for 0.5 seconds, and is still less than half complete.
    bool show_progress_bar = false;

    /// Width of how much has been printed currently into stderr. Used to define size of progress bar and
    /// to check whether progress output needs to be cleared.
    size_t written_progress_chars = 0;

    /// The server periodically sends information about how much data was read since last time.
    /// This information is stored here.
    Progress progress;

    /// Track query execution time.
    Stopwatch watch;

    bool write_progress_on_update = false;
};

}
