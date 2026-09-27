#pragma once

namespace DB
{

class WriteBuffer;

/// Writes `[begin, end)` to `out` with every invalid UTF-8 sequence replaced by U+FFFD, collapsing a run of
/// them into one. Allocates nothing of its own, unlike `WriteBufferValidUTF8`, which buffers into 4 KiB of
/// its own memory so that it can carry a sequence split across two writes. Use this where the whole value is
/// already in hand and that state is not needed.
void writeValidUTF8(const char * begin, const char * end, WriteBuffer & out);

}
