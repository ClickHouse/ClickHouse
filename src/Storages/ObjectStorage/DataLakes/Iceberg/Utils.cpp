
#include "config.h"

#if USE_AVRO

#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#    include <Common/logger_useful.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Iceberg
{

MutableColumns parseAvro(avro::DataFileReaderBase & file_reader, const Block & header, const FormatSettings & settings)
{
    auto deserializer = std::make_unique<AvroDeserializer>(header, file_reader.dataSchema(), true, true, settings);
    MutableColumns columns = header.cloneEmptyColumns();

    file_reader.init();
    RowReadExtension ext;
    while (file_reader.hasMore())
    {
        file_reader.decr();
        deserializer->deserializeRow(columns, file_reader.decoder(), ext);
    }
    return columns;
}


// This function is used to get the file path inside the directory which corresponds to iceberg table from the full blob path which is written in manifest and metadata files.
// For example, if the full blob path is s3://bucket/table_name/data/00000-1-1234567890.avro, the function will return table_name/data/00000-1-1234567890.avro
// Common path should end with "<table_name>" or "<table_name>/".
std::string getFilePath(std::string_view data_path, const std::string & common_path)
{
    using namespace DB;
    auto pos = data_path.find(common_path);
    size_t good_pos = std::string::npos;
    while (pos != std::string::npos)
    {
        auto potential_position = pos + common_path.size();
        if ((std::string_view(data_path.data() + potential_position, 4) == "data")
            || (std::string_view(data_path.data() + potential_position, 5) == "/data")
            || (std::string_view(data_path.data() + potential_position, 8) == "metadata")
            || (std::string_view(data_path.data() + potential_position, 9) == "/metadata"))
        {
            good_pos = pos;
            break;
        }
        size_t new_pos = data_path.find(common_path, pos + 1);
        if (new_pos == std::string::npos)
        {
            break;
        }
        pos = new_pos;
    }


    if (good_pos != std::string::npos)
    {
        return std::string{data_path.substr(good_pos)};
    }
    else if (pos != std::string::npos)
    {
        return std::string{data_path.substr(pos)};
    }
    else
    {
        throw ::DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Expected to find '{}' in data path: '{}'", common_path, data_path);
    }
}
}


#endif
