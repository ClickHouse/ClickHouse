#pragma once

#include <cstddef>
#include <vector>

#include <IO/ReadBuffer.h>
#include <fmt/format.h>
#include "Common/Macros.h"
#include "Common/logger_useful.h"
#include "IO/VarInt.h"
#include "base/scope_guard.h"


namespace DB
{

/// Reads from the concatenation of multiple ReadBuffer's
class ConcatReadBuffer : public ReadBuffer
{
public:
    using Buffers = std::vector<std::unique_ptr<ReadBuffer>>;

    ConcatReadBuffer() : ReadBuffer(nullptr, 0), current(buffers.end())
    {
        logger = getLogger(fmt::format("ConcatReadBuffer {}", size_t(this)));
    }

    explicit ConcatReadBuffer(Buffers && buffers_) : ReadBuffer(nullptr, 0), buffers(std::move(buffers_)), current(buffers.begin())
    {
        assert(!buffers.empty());
        logger = getLogger(fmt::format("ConcatReadBuffer {}", size_t(this)));
        LOG_DEBUG(logger, "vector");
    }

    ConcatReadBuffer(std::unique_ptr<ReadBuffer> buf1, std::unique_ptr<ReadBuffer> buf2) : ConcatReadBuffer()
    {
        LOG_DEBUG(logger, "unique buf1 {}, buf2 {}", size_t(buf1.get()), size_t(buf2.get()));
        appendBuffer(std::move(buf1));
        appendBuffer(std::move(buf2));
    }

    ConcatReadBuffer(ReadBuffer & buf1, ReadBuffer & buf2) : ConcatReadBuffer()
    {
        LOG_DEBUG(logger, "refs buf1 {}, buf2 {}", size_t(&buf1), size_t(&buf2));
        appendBuffer(wrapReadBufferReference(buf1));
        appendBuffer(wrapReadBufferReference(buf2));
    }

    ~ConcatReadBuffer() override
    {
        LOG_DEBUG(logger, "d-tor avalable size: {} offset {} position {} begin {} end {}", available(), offset(), size_t(position()), size_t(working_buffer.begin()), size_t(working_buffer.end()));
    }

    void appendBuffer(std::unique_ptr<ReadBuffer> buffer)
    {
        assert(!count());
        buffers.push_back(std::move(buffer));
        current = buffers.begin();
    }

protected:
    LoggerPtr logger;
    Buffers buffers;
    Buffers::iterator current;

    bool nextImpl() override
    {
        SCOPE_EXIT({
            LOG_DEBUG(logger,
                "nextImpl() finished, available size {}, working buffer size {} has pending data {}, offset {}",
                 available(), working_buffer.size(), hasPendingData(), offset());;
        });

        LOG_DEBUG(logger, "nextImpl() called");

        if (buffers.end() == current)
        {
            LOG_DEBUG(logger, "no buffers left");
            return false;
        }

        /// First reading
        if (working_buffer.empty())
        {
            LOG_DEBUG(logger, "first reading");
            if ((*current)->hasPendingData())
            {
                LOG_DEBUG(logger, "current has data");
                working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
                position() = working_buffer.begin();
                return true;
            }
        }
        else
        {
            LOG_DEBUG(logger, "sync next reading");
            (*current)->position() = position();
        }

        LOG_DEBUG(logger, "next reading");

        if (!(*current)->next())
        {
            LOG_DEBUG(logger, "current has no data, moving to next buffer");

            ++current;
            if (buffers.end() == current)
            {
                LOG_DEBUG(logger, "no more buffers");
                return false;
            }

            LOG_DEBUG(logger, "current available size: {}, bufsize {} offset {} has pos {}", (*current)->available(), (*current)->buffer().size(), (*current)->offset(), bool((*current)->position()));
            /// We skip the filled up buffers; if the buffer is not filled in, but the cursor is at the end, then read the next piece of data.
            while ((*current)->eof())
            {
                LOG_DEBUG(logger, "move to next buffer");
                ++current;
                if (buffers.end() == current)
                {
                    LOG_DEBUG(logger, "no more buffers");
                    return false;
                }
            }
        }

        working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
        position() = working_buffer.begin();
        return true;
    }
};

}
