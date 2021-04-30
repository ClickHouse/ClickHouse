#pragma once

#include <Common/Stopwatch.h>
#include <IO/Progress.h>

/// http://en.wikipedia.org/wiki/ANSI_escape_code
#define CLEAR_TO_END_OF_LINE "\033[K"

namespace DB
{

class ProgressBar
{
public:

    bool updateProgress(Progress & progress, const Progress & value);
    void writeProgress(const Progress & progress, const Stopwatch & watch);
    void clearProgress();

    ///Required Getters
    bool getNeedRenderProgress() const;
    bool getShowProgressBar() const;
    size_t getWrittenProgressChars() const;
    bool getWrittenFirstBlock() const;
    bool getClearProgress() const;

    ///Required Setters
    void setNeedRenderProgress(bool needRenderProgress);
    void setShowProgressBar(bool showProgressBar);
    void setWrittenProgressChars(size_t writtenProgressChars);
    void setWrittenFirstBlock(bool writtenFirstBlock);
    void setClearProgress(bool clearProgress);

private:
    /// The server periodically sends information about how much data was read since last time.
    bool need_render_progress = false;
    /// Render query execution progress.
    bool show_progress_bar = false;

    size_t written_progress_chars = 0;

    bool written_first_block = false;

    bool clear_progress = false;
};

}
