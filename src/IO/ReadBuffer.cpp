#include <IO/ReadBuffer.h>
#include <IO/ReadBufferWrapperBase.h>

#include <Common/Logger.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Core/LogsLevel.h>

#include <exception>


namespace DB
{

namespace ErrorCodes
{
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
            : ReadBuffer(in_.buffer().begin(), in_.buffer().size(), in_.offset()), in(in_), custom_data(std::move(custom_data_))
        {
        }

        const ReadBuffer & getWrappedReadBuffer() const override { return in; }

    private:
        ReadBuffer & in;
        CustomData custom_data;

        bool nextImpl() override
        {
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
}
