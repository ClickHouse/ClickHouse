#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>

#include <mutex>
#include <string>
#include <vector>

namespace DB
{

class WriteBufferFromFileDescriptor;

/** Renders real-time previews of the query result (see the `query_result_previews` setting) in
  * the terminal, below the progress bar line, redrawing the table in place with muted colors.
  * Follows the anchor discipline of `ProgressTable`: every frame starts from the progress bar
  * line, paints downwards, clears the rest of the screen and moves the cursor back up, so the
  * terminal never scrolls.
  *
  * Public methods that take `std::unique_lock<std::mutex> &` expect the caller to hold the lock
  * of the tty output buffer (see `ClientBase::tty_mutex`); the internal state has its own mutex.
  */
class QueryResultPreviewDisplay
{
public:
    QueryResultPreviewDisplay(int in_fd_, int err_fd_) : in_fd(in_fd_), err_fd(err_fd_) {}

    /// Renders the preview block into text lines (using the `PrettyCompactNoEscapes` format),
    /// cutting it to fit the terminal. The block fully replaces the previous preview.
    void setPreview(const Block & block, ContextPtr context);

    /// Paints the current preview below the cursor line and returns the cursor.
    void writePreview(WriteBufferFromFileDescriptor & message, std::unique_lock<std::mutex> &);

    /// Erases the preview from the screen (a no-op when nothing is painted).
    void clearPreviewOutput(WriteBufferFromFileDescriptor & message, std::unique_lock<std::mutex> &);

    /// Forgets the preview at the start of the next query.
    void resetPreview();

private:
    mutable std::mutex mutex;

    std::vector<std::string> lines;
    size_t painted_lines = 0;

    const int in_fd;
    const int err_fd;
};

}
