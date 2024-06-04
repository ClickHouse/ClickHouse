#include <IO/SeekableReadBuffer.h>

#include <istream>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_ISTREAM;
}

namespace
{
    template <typename CustomData>
    class SeekableReadBufferWrapper : public SeekableReadBuffer
    {
    public:
        SeekableReadBufferWrapper(SeekableReadBuffer & in_, CustomData && custom_data_)
            : SeekableReadBuffer(in_.buffer().begin(), in_.buffer().size(), in_.offset())
            , in(in_)
            , custom_data(std::move(custom_data_))
        {
        }

    private:
        SeekableReadBuffer & in;
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

        off_t seek(off_t off, int whence) override
        {
            in.position() = position();
            off_t new_pos = in.seek(off, whence);
            BufferBase::set(in.buffer().begin(), in.buffer().size(), in.offset());
            return new_pos;
        }

        off_t getPosition() override
        {
            in.position() = position();
            return in.getPosition();
        }
    };
}


std::unique_ptr<SeekableReadBuffer> wrapSeekableReadBufferReference(SeekableReadBuffer & ref)
{
    return std::make_unique<SeekableReadBufferWrapper<nullptr_t>>(ref, nullptr);
}

std::unique_ptr<SeekableReadBuffer> wrapSeekableReadBufferPointer(SeekableReadBufferPtr ptr)
{
    return std::make_unique<SeekableReadBufferWrapper<SeekableReadBufferPtr>>(*ptr, SeekableReadBufferPtr{ptr});
}

void copyFromIStreamWithProgressCallback(std::istream & istr, char * to, size_t n, const std::function<bool(size_t)> & progress_callback, size_t * out_bytes_copied, bool * out_cancelled)
{
    const size_t chunk = DBMS_DEFAULT_BUFFER_SIZE;
    if (out_cancelled)
        *out_cancelled = false;

    size_t copied = 0;
    while (copied < n)
    {
        size_t to_copy = std::min(chunk, n - copied);
        istr.read(to + copied, to_copy);
        size_t gcount = istr.gcount();

        copied += gcount;

        bool cancelled = false;
        if (gcount && progress_callback)
            cancelled = progress_callback(copied);
        *out_bytes_copied = copied;

        if (gcount != to_copy)
        {
            if (!istr.eof())
                throw Exception(
                    ErrorCodes::CANNOT_READ_FROM_ISTREAM,
                    "{} at offset {}",
                    istr.fail() ? "Cannot read from istream" : "Unexpected state of istream",
                    copied);

            break;
        }

        if (cancelled)
        {
            if (out_cancelled != nullptr)
                *out_cancelled = true;
            break;
        }
    }

    *out_bytes_copied = copied;
}

}
