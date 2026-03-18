#include <Storages/Distributed/DistributedAsyncInsertSource.h>
#include <Storages/Distributed/DistributedAsyncInsertHeader.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeReader.h>
#include <Poco/Logger.h>

namespace DB
{

struct DistributedAsyncInsertSource::Data
{
    LoggerPtr log = nullptr;

    ReadBufferFromFile in;
    CompressedReadBuffer decompressing_in;
    NativeReader block_in;

    Block first_block;

    explicit Data(const String & file_name)
        : log(getLogger("DistributedAsyncInsertSource"))
        , in(file_name)
        , decompressing_in(in)
        , block_in(decompressing_in, DistributedAsyncInsertHeader::read(in, log).revision)
        , first_block(block_in.read())
    {
    }
};

DistributedAsyncInsertSource::DistributedAsyncInsertSource(const String & file_name)
    : DistributedAsyncInsertSource(std::make_unique<Data>(file_name))
{
}

DistributedAsyncInsertSource::DistributedAsyncInsertSource(std::unique_ptr<Data> data_)
    : ISource(data_->first_block.cloneEmpty())
    , data(std::move(data_))
{
}

DistributedAsyncInsertSource::~DistributedAsyncInsertSource() = default;

Chunk DistributedAsyncInsertSource::generate()
{
    if (data->first_block)
    {
        size_t num_rows = data->first_block.rows();
        Chunk res(data->first_block.getColumns(), num_rows);
        data->first_block.clear();
        return res;
    }

    auto block = data->block_in.read();
    if (!block)
        return {};

    size_t num_rows = block.rows();
    return Chunk(block.getColumns(), num_rows);
}

}
