#pragma once

#include <Common/Stopwatch.h>
#include <IO/Progress.h>

/// http://en.wikipedia.org/wiki/ANSI_escape_code
#define CLEAR_TO_END_OF_LINE "\033[K"

namespace DB
{

struct ProgressBar
{
public:

    bool updateProgress(Progress & progress, const Progress & value);
    void writeProgress(const Progress & progress, const Stopwatch & watch);
    void clearProgress();

    bool need_render_progress = false;

    bool show_progress_bar = false;

    size_t written_progress_chars = 0;

    bool written_first_block = false;

    bool clear_progress = false;
};

}
