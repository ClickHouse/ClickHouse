#include "ORCBlockInputFormat.h"
#if USE_ORC

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/adapters/orc/adapter.h>
#include <arrow/io/memory.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
}

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

ORCBlockInputFormat::ORCBlockInputFormat(ReadBuffer & in_, Block header_) : IInputFormat(std::move(header_), in_)
{
}

Chunk ORCBlockInputFormat::generate()
{
    Chunk res;
    const Block & header = getPort().getHeader();

    if (!file_reader)
        prepareReader();

    if (stripe_current >= stripe_total)
        return res;

    std::shared_ptr<arrow::RecordBatch> batch_result;
    arrow::Status batch_status = file_reader->ReadStripe(stripe_current, include_indices, &batch_result);
    if (!batch_status.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA,
                               "Error while reading batch of ORC data: {}", batch_status.ToString());

    auto table_result = arrow::Table::FromRecordBatches({batch_result});
    if (!table_result.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA,
                               "Error while reading batch of ORC data: {}", table_result.status().ToString());

    ++stripe_current;

    ArrowColumnToCHColumn::arrowTableToCHChunk(res, *table_result, header, "ORC");
    return res;
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    include_indices.clear();
    stripe_current = 0;
}

size_t countIndicesForType(std::shared_ptr<arrow::DataType> type)
{
    if (type->id() == arrow::Type::LIST)
        return countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type()) + 1;

    return 1;
}

void ORCBlockInputFormat::prepareReader()
{
    THROW_ARROW_NOT_OK(arrow::adapters::orc::ORCFileReader::Open(asArrowFile(in), arrow::default_memory_pool(), &file_reader));
    stripe_total = file_reader->NumberOfStripes();
    stripe_current = 0;

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(file_reader->ReadSchema(&schema));

    int index = 0;
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        if (getPort().getHeader().has(schema->field(i)->name()))
        {
            /// LIST type require 2 indices, so we should recursively
            /// count the number of indices we need for this type.
            int indexes_count = countIndicesForType(schema->field(i)->type());
            for (int j = 0; j != indexes_count; ++j)
                include_indices.push_back(index++);
        }
    }
}

void registerInputFormatProcessorORC(FormatFactory &factory)
{
    factory.registerInputFormatProcessor(
            "ORC",
            [](ReadBuffer &buf,
                const Block &sample,
                const RowInputFormatParams &,
                const FormatSettings & /* settings */)
            {
                return std::make_shared<ORCBlockInputFormat>(buf, sample);
            });
    factory.markFormatAsColumnOriented("ORC");
}

}
#else

namespace DB
{
    class FormatFactory;
    void registerInputFormatProcessorORC(FormatFactory &)
    {
    }
}

#endif
