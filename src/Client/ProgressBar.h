#pragma once

#include "../Common/Stopwatch.h"
#include "../IO/Progress.h"


namespace DB
{

class ProgressBar
{
public:

    bool updateProgress(Progress & progress, const Progress &value);
    void writeProgress(const Progress & progress, const Stopwatch & watch);

    ///Required Getters
    bool getNeedRenderProgress() const;
    bool getShowProgressBar() const;
    size_t getWrittenProgressChars() const;
    bool isWrittenFirstBlock() const;

    ///Required Setters
    void setNeedRenderProgress(bool needRenderProgress);
    void setShowProgressBar(bool showProgressBar);
    void setWrittenProgressChars(size_t writtenProgressChars);
    void setWrittenFirstBlock(bool writtenFirstBlock);

private:
    /// The server periodically sends information about how much data was read since last time.
    bool need_render_progress = false;
    /// Render query execution progress.
    bool show_progress_bar = false;

    size_t written_progress_chars = 0;
    bool written_first_block = false;
};

}
