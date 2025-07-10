#pragma once
#include <vector>
#include <IO/WriteBuffer.h>


namespace DB
{

/** DynamicWriteBufferManager manage vector of WriteBufferPtr.
  * Unlike ForkWriteBuffer it can not be used directly to write data:
  * next/write should not be called on this buffer.
  * But it is useful to proxy finalize, cancel and sync calls to all
  * WriteBuffers managed by this buffer
  **/
class DynamicWriteBufferManager : public WriteBuffer
{
public:
    DynamicWriteBufferManager();
    void addWriteBuffer(WriteBufferPtr && buffer);
    void proxyNext();

    void sync() override;
protected:
    void finalizeImpl() override;
    void cancelImpl() noexcept override;

private:
    void nextImpl() override;

    std::vector<WriteBufferPtr> sources;
};

}
