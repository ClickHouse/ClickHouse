#pragma once

#include "config.h"

#if USE_AVRO

#include <IO/ReadBufferFromFileBase.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeTuple.h>
#include <Core/Field.h>

#include <memory>

namespace DB::Iceberg
{

/// In Iceberg manifest files and manifest lists are store in Avro format: https://avro.apache.org/
/// This format is some kind of mix between JSON and binary schemaful format like protobuf.
/// It has rich types system, with it's own binary representation and it's really tricky
/// to parse some of them.
///
/// In ClickHouse we already support avro as input format, so we can parse it. The main complexity
/// comes from the fact that we parse Avro files into nested Tuple column which are really hard
/// to operate in key-value fashion. That is why this class is written on top of our avro parser.
/// It allows to access files in avro files using syntax like "data_file.partition.XXXX" and return
/// Field values back. Also manages avro file metadata which is basically just mapping string -> string.
class AvroForIcebergDeserializer
{
private:
    std::unique_ptr<DB::ReadBufferFromFileBase> buffer;
    std::string manifest_file_path;
    DB::ColumnPtr parsed_column;
    std::shared_ptr<const DB::DataTypeTuple> parsed_column_data_type;

    std::map<std::string, std::vector<uint8_t>> metadata;
public:

    AvroForIcebergDeserializer(
        std::unique_ptr<DB::ReadBufferFromFileBase> buffer_,
        const std::string & manifest_file_path_,
        const DB::FormatSettings & format_settings);

    size_t rows() const;

    /// Allow to access avro paths like "a.b.c"
    bool hasPath(const std::string & path) const;
    DB::TypeIndex getTypeForPath(const std::string & path) const;
    /// Allow to access avro paths like "a.b.c".
    /// If expected type is provided will throw an exception if types don't match
    DB::Field getValueFromRowByName(size_t row_num, const std::string & path, std::optional<DB::TypeIndex> expected_type = std::nullopt) const;

    std::optional<std::string> tryGetAvroMetadataValue(std::string metadata_key) const;

    String getContent(size_t row_number) const;
    String getMetadataContent() const;
};

}

#endif
