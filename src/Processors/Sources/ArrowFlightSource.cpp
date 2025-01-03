#include "ArrowFlightSource.h"

#include <Processors/Chunk.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <arrow/array.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_EXCEPTION;
    extern const int ARROWFLIGHT_CONNECTION_FAILURE;
    extern const int ARROWFLIGHT_FETCH_SCHEMA_ERROR;
    extern const int ARROWFLIGHT_INTERNAL_ERROR;
}

ArrowFlightSource::ArrowFlightSource(
    std::unique_ptr<arrow::flight::FlightClient> & client_,
    const std::string & query_,
    const Block & sample_block_,
    const std::vector<std::string> & column_names_,
    UInt64 max_block_size_)
    : ISource(sample_block_.cloneEmpty())
    , client(std::move(client_))
    , query(query_)
    , max_block_size(max_block_size_)
    , sample_block(sample_block_)
    , column_names(column_names_)
{
    initializeStream();
}

void ArrowFlightSource::initializeStream()
{
    arrow::flight::FlightCallOptions options;
    arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Command(query);

    arrow::flight::Ticket ticket;
    ticket.ticket = query;
    auto result = client->DoGet(options, ticket);
    if (!result.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE,
            "Failed to initialize Arrow Flight stream: {}",
            result.status().ToString()
        );
    }

    stream_reader = std::move(result.ValueOrDie());

    auto result_schema = stream_reader->GetSchema();
    if (!result_schema.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR,
            "Failed to get table schema: {}",
            result_schema.status().ToString()
        );
    }
    schema = result_schema.ValueOrDie();
}

Chunk ArrowFlightSource::generate()
{
    auto status = stream_reader->Next();

    if (!status.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_INTERNAL_ERROR,
            "Arrow Flight internal error: {}",
            status.status().ToString()
        );
    }

    auto chunk = status.ValueOrDie();
    auto batch = chunk.data;

    if (!batch)
    {
        return {};
    }

    MutableColumns columns = sample_block.cloneEmptyColumns();
    
    fillChunk(columns, batch);

    Chunk filled_chunk(std::move(columns), batch->num_rows());
    return filled_chunk;
}

void ArrowFlightSource::fillChunk(MutableColumns & columns, std::shared_ptr<arrow::RecordBatch> & batch)
{
    for (int col_idx = 0; col_idx < batch->num_columns(); ++col_idx)
    {
        auto & column = columns[col_idx];
        const auto & arrow_column = batch->column(col_idx);

        for (int64_t row_idx = 0; row_idx < arrow_column->length(); ++row_idx)
        {
            if (arrow_column->IsNull(row_idx))
            {
                column->insertDefault();
            }
            else
            {
                switch (arrow_column->type()->id())
                {
                    case arrow::Type::STRING:
                    {
                        const auto & str_array = std::static_pointer_cast<arrow::StringArray>(arrow_column);
                        column->insertData(str_array->GetView(row_idx).data(), str_array->GetView(row_idx).size());
                        break;
                    }
                    // TODO: add more types support
                    default:
                        throw Exception(
                            ErrorCodes::NOT_IMPLEMENTED,
                            "Arrow type {} not implemented",
                            arrow_column->type()->ToString()
                        );
                }
            }
        }
    }
}

}
