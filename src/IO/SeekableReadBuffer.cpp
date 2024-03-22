#include <IO/SeekableReadBuffer.h>


namespace DB
{

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

}
