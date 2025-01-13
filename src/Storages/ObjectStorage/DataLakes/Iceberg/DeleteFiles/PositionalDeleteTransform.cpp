#include <Storages/ObjectStorage/DataLakes/Iceberg/DeleteFiles/PositionalDeleteTransform.h>

#include <Common/Exception.h>
#include <Columns/IColumn.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

PositionalDeleteTransform::PositionalDeleteTransform(
    const Block & header_, std::vector<std::shared_ptr<IInputFormat>> delete_file_sources_, const std::string & source_filename_)
    : ISimpleTransform(header_, header_, false)
    , delete_file_sources(delete_file_sources_)
    , source_filename(cropPrefix(source_filename_))
{
    unprocessed_delete_chunk.resize(delete_file_sources_.size());
    filename_column_index.resize(delete_file_sources_.size());
    position_column_index.resize(delete_file_sources_.size());
}

void PositionalDeleteTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    IColumn::Filter should_delete(num_rows, true);
    size_t num_rows_after_filtration = num_rows;

    for (size_t delete_source_id = 0; delete_source_id < delete_file_sources.size(); ++delete_source_id)
    {
        while (true)
        {
            auto delete_chunk = readNextDeleteChunk(delete_source_id);
            if (!delete_chunk)
                break;

            int position_index = getPositionColumnIndex(delete_source_id);
            int filename_index = getFilenameColumnIndex(delete_source_id);

            auto position_column = delete_chunk.getColumns()[position_index];
            auto filename_column = delete_chunk.getColumns()[filename_index];
            auto last_filename = filename_column->getDataAt(delete_chunk.getNumRows() - 1).toString();
            last_filename = cropPrefix(std::move(last_filename));
            if (last_filename < source_filename)
                break;

            auto last_position = position_column->get64(delete_chunk.getNumRows() - 1);
            if (last_position < chunk_rows_iterator && last_filename == source_filename)
                break;

            auto first_filename = filename_column->getDataAt(0).toString();
            first_filename = cropPrefix(std::move(first_filename));
            if (first_filename > source_filename)
            {
                unprocessed_delete_chunk[delete_source_id] = std::move(delete_chunk);
                break;
            }

            auto first_position = position_column->get64(0);
            if (first_position >= chunk_rows_iterator + chunk.getNumRows() && last_filename == source_filename)
            {
                unprocessed_delete_chunk[delete_source_id] = std::move(delete_chunk);
                break;
            }

            bool should_cache = false;
            for (size_t i = 0; i < delete_chunk.getNumRows(); ++i)
            {
                auto position_to_delete = position_column->get64(i);
                auto filename_to_delete = filename_column->getDataAt(i).toString();
                if (cropPrefix(std::move(filename_to_delete)) == source_filename)
                {
                    if (position_to_delete - chunk_rows_iterator < chunk.getNumRows())
                    {
                        should_delete[position_to_delete - chunk_rows_iterator] = false;
                        --num_rows_after_filtration;
                    }
                    else
                    {
                        should_cache = true;
                    }
                }
            }
            if (should_cache)
            {
                unprocessed_delete_chunk[delete_source_id] = std::move(delete_chunk);
                break;
            }
        }
    }

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->filter(should_delete, -1);

    chunk.setColumns(std::move(columns), num_rows_after_filtration);
    chunk_rows_iterator += num_rows;
}

Chunk PositionalDeleteTransform::readNextDeleteChunk(size_t source_id)
{
    if (unprocessed_delete_chunk[source_id])
    {
        Chunk result = std::move(*unprocessed_delete_chunk[source_id]);
        unprocessed_delete_chunk[source_id] = std::nullopt;
        return result;
    }
    return delete_file_sources[source_id]->read();
}

int PositionalDeleteTransform::getFilenameColumnIndex(size_t delete_source_id)
{
    if (!filename_column_index[delete_source_id])
    {
        const auto & delete_header = delete_file_sources[delete_source_id]->getOutputs().back().getHeader();
        for (size_t i = 0; i < delete_header.getNames().size(); ++i)
        {
            if (delete_header.getNames()[i] == filename_column_name)
            {
                filename_column_index[delete_source_id] = i;
                break;
            }
        }
        if (!filename_column_index[delete_source_id])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not find column {} in chunk", filename_column_name);
    }
    return *filename_column_index[delete_source_id];
}

int PositionalDeleteTransform::getPositionColumnIndex(size_t delete_source_id)
{
    if (!position_column_index[delete_source_id])
    {
        const auto & delete_header = delete_file_sources[delete_source_id]->getOutputs().back().getHeader();
        for (size_t i = 0; i < delete_header.getNames().size(); ++i)
        {
            if (delete_header.getNames()[i] == positions_column_name)
            {
                position_column_index[delete_source_id] = i;
                break;
            }
        }
        if (!position_column_index[delete_source_id])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not find column {} in chunk", positions_column_name);
    }
    return *position_column_index[delete_source_id];
}

}
