#pragma once

#include "config.h"

#if USE_BASE64

#include <Core/Defines.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>

#include <libbase64.h>

namespace DB
{

/// Encodes everything written to it as standard base64 (with `=` padding, no line breaks) and streams the
/// result into the underlying buffer. Unlike the compression write buffers it does not finalize the underlying
/// buffer, so the caller can keep writing raw bytes to it after `finalize()` (e.g. terminal control sequences
/// that wrap the encoded payload).
class Base64WriteBuffer final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit Base64WriteBuffer(WriteBuffer & out_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);
    ~Base64WriteBuffer() override;

private:
    void nextImpl() override;
    void finalizeImpl() override;

    WriteBuffer & out;
    base64_state state{};
};

}

#endif
