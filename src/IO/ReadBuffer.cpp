#include <IO/ReadBuffer.h>
#include <IO/ReadBufferWrapperBase.h>

#include <Common/Logger.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <base/scope_guard.h>
#include <Core/LogsLevel.h>

#include <cstddef>
#include <exception>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
}

namespace
{
    template <typename CustomData>
    class ReadBufferWrapper : public ReadBuffer, public ReadBufferWrapperBase
    {
    public:
        ReadBufferWrapper(ReadBuffer & in_, CustomData && custom_data_)
            : ReadBuffer(nullptr, 0, 0)
            , in(in_)
            , custom_data(std::move(custom_data_))
        {
            logger = getLogger(fmt::format("ReadBufferWrapper {}", size_t(this)));

            working_buffer = Buffer(in.position(), in.buffer().end());
            position() = working_buffer.begin();

            LOG_DEBUG(logger,
                "c-tor & id {} self id {} available size: {} offset {} size {} position {} begin {} end {}",
                size_t(&in), size_t(this), available(), offset(), buffer().size(), size_t(position()), size_t(working_buffer.begin()), size_t(working_buffer.end()));
        }

        const ReadBuffer & getWrappedReadBuffer() const override { return in; }

    private:
        LoggerPtr logger;
        ReadBuffer & in;
        CustomData custom_data;

        bool nextImpl() override
        {
            // LOG_DEBUG(logger,
            //     "nextImpl available size: {} offset {}, size {} position {} begin {} end {}",
            //     available(), offset(), buffer().size(), size_t(position()), size_t(working_buffer.begin()), size_t(working_buffer.end()));
            // LOG_DEBUG(logger,
            //     "nextImpl in::: available size: {} offset {}, size {} position {} begin {} end {}",
            //     in.available(), in.offset(), in.buffer().size(), size_t(in.position()), size_t(in.buffer().begin()), size_t(in.buffer().end()));

            // SCOPE_EXIT({
            //     LOG_DEBUG(logger, "next() finished, available size {}, working buffer size {} offset {}",
            //       available(), buffer().size(), offset());
            // });

            in.position() = position();
            if (!in.next())
            {
                set(in.position(), 0);
                return false;
            }
            BufferBase::set(in.buffer().begin(), in.buffer().size(), in.offset());
            return true;
        }
    };
}

void ReadBuffer::readStrict(char * to, size_t n)
{
    auto read_bytes = read(to, n);
    if (n != read_bytes)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "Cannot read all data. Bytes read: {}. Bytes expected: {}.", read_bytes, std::to_string(n));
}

void ReadBuffer::throwReadAfterEOF()
{
    throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Attempt to read after eof");
}

std::unique_ptr<ReadBuffer> wrapReadBufferReference(ReadBuffer & ref)
{
    return std::make_unique<ReadBufferWrapper<nullptr_t>>(ref, nullptr);
}

std::unique_ptr<ReadBuffer> wrapReadBufferPointer(ReadBufferPtr ptr)
{
    return std::make_unique<ReadBufferWrapper<ReadBufferPtr>>(*ptr, ReadBufferPtr{ptr});
}

void ReadBuffer::cancel()
{
    if (std::current_exception())
        tryLogCurrentException(getLogger("ReadBuffer"), "ReadBuffer is canceled by the exception", LogsLevel::debug);
    else
        LOG_DEBUG(getLogger("ReadBuffer"), "ReadBuffer is canceled at {}", StackTrace().toString());

    canceled = true;
}

bool ReadBuffer::next()
{
    if (hasPendingData())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ReadBuffer: hasPendingData, but next() called: id {}, available {}, offset {} size {} position {}  bigin {} end {}",
            size_t(this),
            available(),
            offset(),
            buffer().size(),
            size_t(position()),
            size_t(working_buffer.begin()),
            size_t(working_buffer.end()));
    }

    // SCOPE_EXIT({
    //     LOG_DEBUG(getLogger("ReadBuffer"),
    //         "id {} next() finished, available size {}, working buffer size {} has pending data {} offset {} position {} bigin {} end {}",
    //               size_t(this), available(), working_buffer.size(), hasPendingData(), offset(), size_t(position()), size_t(working_buffer.begin()), size_t(working_buffer.end()));
    // });

    chassert(!hasPendingData());
    chassert(position() <= working_buffer.end());
    chassert(!isCanceled(), "ReadBuffer is canceled. Can't read from it.");

    bytes += offset();
    bool res = false;
    try
    {
        res = nextImpl();
    }
    catch (...)
    {
        cancel();
        throw;
    }

    if (!res)
    {
        working_buffer = Buffer(pos, pos);
    }
    else
    {
        pos = working_buffer.begin() + std::min(nextimpl_working_buffer_offset, working_buffer.size());
        chassert(position() < working_buffer.end());
    }
    nextimpl_working_buffer_offset = 0;

    chassert(position() <= working_buffer.end());

    return res;
}
}
