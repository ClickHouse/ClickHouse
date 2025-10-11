#include <optional>
#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>
#include <IO/WithFileName.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ChunkInfoRowNumbers::ChunkInfoRowNumbers(size_t row_num_offset_, std::optional<IColumnFilter> applied_filter_)
    : row_num_offset(row_num_offset_), applied_filter(std::move(applied_filter_)) { }

ChunkInfoRowNumbers::Ptr ChunkInfoRowNumbers::clone() const
{
    auto res = std::make_shared<ChunkInfoRowNumbers>(row_num_offset);
    if (applied_filter.has_value())
        res->applied_filter.emplace(applied_filter->begin(), applied_filter->end());
    return res;
}

IInputFormat::IInputFormat(SharedHeader header, ReadBuffer * in_) : ISource(std::move(header)), in(in_)
{
    column_mapping = std::make_shared<ColumnMapping>();
}

Chunk IInputFormat::generate()
{
    try
    {
        Chunk res = read();
        return res;
    }
    catch (Exception & e)
    {
        auto file_name = getFileNameFromReadBuffer(getReadBuffer());
        if (!file_name.empty())
            e.addMessage(fmt::format("(in file/uri {})", file_name));
        throw;
    }
}

void IInputFormat::resetParser()
{
    if (in)
        in->ignoreAll();

    // those are protected attributes from ISource (I didn't want to propagate resetParser up there)
    finished = false;
    got_exception = false;

    getPort().getInputPort().reopen();
}

void IInputFormat::setReadBuffer(ReadBuffer & in_)
{
    in = &in_;
}

Chunk IInputFormat::getChunkForCount(size_t rows)
{
    const auto & header = getPort().getHeader();
    return cloneConstWithDefault(Chunk{header.getColumns(), 0}, rows);
}

void IInputFormat::resetOwnedBuffers()
{
    owned_buffers.clear();
}

void IInputFormat::onFinish()
{
    resetReadBuffer();
}

void IInputFormat::setBucketsToRead(const std::vector<size_t> & /*buckets_to_read*/)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not skip chunks for format {}", getName());
}

std::optional<std::vector<size_t>> IInputFormat::getChunksByteSizes()
{
    return std::nullopt;
}

}
