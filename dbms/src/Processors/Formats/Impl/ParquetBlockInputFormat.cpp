#include "ParquetBlockInputFormat.h"
#if USE_PARQUET

#include <Formats/FormatFactory.h>
#include <IO/BufferBase.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include "ArrowColumnToCHColumn.h"

namespace DB
{

    ParquetBlockInputFormat::ParquetBlockInputFormat(ReadBuffer & in_, Block header_)
            : IInputFormat(std::move(header_), in_)
    {
    }

    Chunk ParquetBlockInputFormat::generate()
    {
        Chunk res;
        auto &header = getPort().getHeader();

        if (!in.eof())
        {
            /*
               First we load whole stream into string (its very bad and limiting .parquet file size to half? of RAM)
               Then producing blocks for every row_group (dont load big .parquet files with one row_group - it can eat x10+ RAM from .parquet file size)
            */

            if (row_group_current < row_group_total)
                throw Exception{"Got new data, but data from previous chunks was not read " +
                                std::to_string(row_group_current) + "/" + std::to_string(row_group_total),
                                ErrorCodes::CANNOT_READ_ALL_DATA};

            file_data.clear();
            {
                WriteBufferFromString file_buffer(file_data);
                copyData(in, file_buffer);
            }

            buffer = std::make_unique<arrow::Buffer>(file_data);
            // TODO: maybe use parquet::RandomAccessSource?
            auto reader = parquet::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(*buffer));
            file_reader = std::make_unique<parquet::arrow::FileReader>(::arrow::default_memory_pool(),
                                                                       std::move(reader));
            row_group_total = file_reader->num_row_groups();
            row_group_current = 0;
        }
        //DUMP(row_group_current, row_group_total);
        if (row_group_current >= row_group_total)
            return res;

        // TODO: also catch a ParquetException thrown by filereader?
        //arrow::Status read_status = filereader.ReadTable(&table);
        std::shared_ptr<arrow::Table> table;
        arrow::Status read_status = file_reader->ReadRowGroup(row_group_current, &table);

        ArrowColumnToCHColumn::arrowTableToCHChunk(res, table, read_status, header, row_group_current, "Parquet");
        return res;
    }

    void ParquetBlockInputFormat::resetParser()
    {
        IInputFormat::resetParser();

        file_reader.reset();
        file_data.clear();
        buffer.reset();
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
