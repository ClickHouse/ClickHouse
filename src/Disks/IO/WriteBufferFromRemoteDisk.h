#pragma once

#include <string>

#include <IO/WriteBufferFromFileBase.h>
#include <Client/RemoteFSConnectionPool.h>

namespace DB
{

class WriteBufferFromRemoteDisk : public WriteBufferFromFileBase
{
public:
    explicit WriteBufferFromRemoteDisk(
        RemoteFSConnectionPool::Entry & conn_,
        std::string file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~WriteBufferFromRemoteDisk() override;

    void sync() override;

    std::string getFileName() const override;

protected:
    void nextImpl() override;

    RemoteFSConnectionPool::Entry conn;

    std::string file_name;

    void finalizeImpl() override;
};

}
