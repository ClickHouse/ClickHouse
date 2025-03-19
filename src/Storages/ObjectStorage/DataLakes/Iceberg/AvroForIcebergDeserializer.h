#pragma once

#include "config.h"

#if USE_AVRO

#include <IO/ReadBufferFromFileBase.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeTuple.h>
#include <Core/Field.h>

#include <memory>

namespace Iceberg
{

class AvroForIcebergDeserializer
{
private:
    std::unique_ptr<DB::ReadBufferFromFileBase> buffer;
    std::string manifest_file_path;
    DB::ColumnPtr parsed_column;
    std::shared_ptr<const DB::DataTypeTuple> parsed_column_data_type;

    std::map<std::string, std::vector<uint8_t>> metadata;
public:

    explicit AvroForIcebergDeserializer(
        std::unique_ptr<DB::ReadBufferFromFileBase> buffer_,
        const std::string & manifest_file_path_,
        const DB::FormatSettings & format_settings);

    size_t rows() const;

    bool hasPath(const std::string & path) const;
    DB::TypeIndex getTypeForPath(const std::string & path) const;
    DB::Field getValueFromRowByName(size_t row_num, const std::string & path, std::optional<DB::TypeIndex> expected_type = std::nullopt) const;

    std::optional<std::string> tryGetAvroMetadataValue(std::string metadata_key) const;

};
   
}

#endif
