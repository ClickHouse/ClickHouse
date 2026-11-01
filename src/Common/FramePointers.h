#pragma once

#include <array>

/* NOTE: It cannot be larger right now, since otherwise it
 * will not fit into minimal PIPE_BUF (512) in TraceCollector.
 */
static constexpr size_t FRAMEPOINTER_CAPACITY = 45;

using FramePointers = std::array<void *, FRAMEPOINTER_CAPACITY>;
