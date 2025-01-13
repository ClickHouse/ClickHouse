#pragma once

#include <cstddef>
#include <optional>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/ISimpleTransform.h>
#include <Common/PODArray.h>

namespace DB
{

class PositionalDeleteTransform : public ISimpleTransform
{
public:
    static constexpr const char * positions_column_name = "pos";
    static constexpr const char * filename_column_name = "file_path";

    PositionalDeleteTransform(
        const Block & header_, std::vector<std::shared_ptr<IInputFormat>> delete_file_sources_, const std::string & source_filename_);

    String getName() const override { return "PositionalDeleteTransform"; }
    void setDescription(const String & str) { description = str; }

protected:
    void transform(Chunk & chunk) override;

private:
    std::vector<std::shared_ptr<IInputFormat>> delete_file_sources;
    String description;
    Block header;

    std::vector<std::optional<Chunk>> unprocessed_delete_chunk;
    size_t chunk_rows_iterator = 0;

    std::vector<std::optional<int>> filename_column_index;
    std::vector<std::optional<int>> position_column_index;
    std::string source_filename;

    Chunk readNextDeleteChunk(size_t source_id);

    std::string cropPrefix(std::string path) const
    {
        if (path[0] == '/')
            return path.substr(1);
        return path;
    }

    int getFilenameColumnIndex(size_t delete_source_id);
    int getPositionColumnIndex(size_t delete_source_id);
};

}
