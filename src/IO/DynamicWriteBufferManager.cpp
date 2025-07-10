#include "DynamicWriteBufferManager.h"

#include <Common/Exception.h>
#include "IO/WriteBuffer.h"

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

DynamicWriteBufferManager::DynamicWriteBufferManager() : WriteBuffer(nullptr, 0)
{
}

void DynamicWriteBufferManager::addWriteBuffer(WriteBufferPtr && buffer)
{
    sources.push_back(std::move(buffer));
}

void DynamicWriteBufferManager::proxyNext()
{
    for (const WriteBufferPtr & buffer : sources)
    {
        buffer->next();
    }
}

void DynamicWriteBufferManager::nextImpl()
{
    throw Exception{ErrorCodes::LOGICAL_ERROR, "Cannot write to DynamicWriteBufferManager"};
}

void DynamicWriteBufferManager::finalizeImpl()
{
    for (const WriteBufferPtr & buffer : sources)
    {
        buffer->finalize();
    }
}

void DynamicWriteBufferManager::cancelImpl() noexcept
{
    for (const WriteBufferPtr & buffer : sources)
    {
        buffer->cancel();
    }
}

void DynamicWriteBufferManager::sync()
{
    for (const WriteBufferPtr & buffer : sources)
    {
        buffer->sync();
    }
}
}
