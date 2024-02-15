#include <IO/ReadBuffer.h>
#include <IO/ReadBufferWrapperBase.h>


namespace DB
{

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


std::unique_ptr<ReadBuffer> wrapReadBufferReference(ReadBuffer & ref)
{
    return std::make_unique<ReadBufferWrapper<nullptr_t>>(ref, nullptr);
}

std::unique_ptr<ReadBuffer> wrapReadBufferPointer(ReadBufferPtr ptr)
{
    return std::make_unique<ReadBufferWrapper<ReadBufferPtr>>(*ptr, ReadBufferPtr{ptr});
}

}
