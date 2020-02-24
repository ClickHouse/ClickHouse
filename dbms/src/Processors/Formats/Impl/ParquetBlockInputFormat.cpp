#include "ParquetBlockInputFormat.h"
#if USE_PARQUET

#include <Formats/FormatFactory.h>
#include <IO/BufferBase.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include "ArrowColumnToCHColumn.h"

#include <common/logger_useful.h>


#include <sys/stat.h>

namespace DB
{

class RandomAccessFileFromSeekableReadBuffer : public arrow::io::RandomAccessFile
{
public:
    RandomAccessFileFromSeekableReadBuffer(SeekableReadBuffer& in_, off_t file_size_)
        : in(in_)
        , file_size(file_size_)
        , is_closed(false)
    {

    }

    virtual arrow::Status GetSize(int64_t* size) override
    {
        *size = file_size;
        return arrow::Status::OK();
    }

    virtual arrow::Status Close() override
    {
        is_closed = true;
        return arrow::Status::OK();
    }

    virtual arrow::Status Tell(int64_t* position) const override
    {
        *position = in.getPosition();
        return arrow::Status::OK();
    }

    virtual bool closed() const override { return is_closed; }

    virtual arrow::Status Read(int64_t nbytes, int64_t* bytes_read, void* out) override
    {
        *bytes_read = in.readBig(reinterpret_cast<char *>(out), nbytes);
        return arrow::Status::OK();
    }

    virtual arrow::Status Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) override
    {
        std::shared_ptr<arrow::Buffer> buf;
        ARROW_RETURN_NOT_OK(arrow::AllocateBuffer(nbytes, &buf));
        size_t n = in.readBig(reinterpret_cast<char *>(buf->mutable_data()), nbytes);
        *out = arrow::SliceBuffer(buf, 0, n);
        return arrow::Status::OK();
    }

    virtual arrow::Status Seek(int64_t position) override
    {
        in.seek(position, SEEK_SET);
        return arrow::Status::OK();
    }

private:
    SeekableReadBuffer& in;
    off_t file_size;
    bool is_closed;
};


static std::shared_ptr<arrow::io::RandomAccessFile>  as_arrow_file(ReadBuffer & in)
{
    if (auto fd_in = dynamic_cast<ReadBufferFromFileDescriptor*>(&in))
    {
        struct stat stat;
        auto res = ::fstat(fd_in->getFD(), &stat);
        // if fd is a regular file i.e. not stdin
        if (res == 0 && S_ISREG(stat.st_mode))
        {
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*fd_in, stat.st_size);
        }
    }

    // fallback to loading the entire file in memory
    std::string file_data;
    {
        WriteBufferFromString file_buffer(file_data);
        copyData(in, file_buffer);
    }
    return std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(file_data)));
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

ParquetBlockInputFormat::ParquetBlockInputFormat(ReadBuffer & in_, Block header_)
    : IInputFormat(std::move(header_), in_)
{
    THROW_ARROW_NOT_OK(parquet::arrow::OpenFile(as_arrow_file(in_), arrow::default_memory_pool(), &file_reader));
    row_group_total = file_reader->num_row_groups();

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(file_reader->GetSchema(&schema));

    for (int i = 0; i < schema->num_fields(); ++i)
    {
        if (getPort().getHeader().has(schema->field(i)->name()))
        {
            column_indices.push_back(i);
        }
    }
}

Chunk ParquetBlockInputFormat::generate()
{
    Chunk res;
    auto &header = getPort().getHeader();

    if (row_group_current >= row_group_total)
        return res;

    std::shared_ptr<arrow::Table> table;
    arrow::Status read_status = file_reader->ReadRowGroup(row_group_current, column_indices, &table);
    ArrowColumnToCHColumn::arrowTableToCHChunk(res, table, read_status, header, row_group_current, "Parquet");
    return res;
}

void ParquetBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    row_group_total = 0;
    row_group_current = 0;
}

void registerInputFormatProcessorParquet(FormatFactory &factory)
{
    factory.registerInputFormatProcessor(
            "Parquet",
            [](ReadBuffer &buf,
                const Block &sample,
                const RowInputFormatParams &,
                const FormatSettings & /* settings */)
            {
                return std::make_shared<ParquetBlockInputFormat>(buf, sample);
            });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProcessorParquet(FormatFactory &)
{
}
}

#endif
