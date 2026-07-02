#include <filesystem>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::Iceberg
{

// This function is used to get the file path inside the directory which corresponds to Iceberg table from the full blob path which is written in manifest and metadata files.
// For example, if the full blob path is s3://bucket/table_name/data/00000-1-1234567890.avro, the function will return table_name/data/00000-1-1234567890.avro
// Common path should end with "<table_name>" or "<table_name>/".
String IcebergPathResolver::resolve(const IcebergPathFromMetadata & metadata_path) const
{
    auto trim_forward_slash = [](std::string_view str) -> std::string_view
    {
        if (str.starts_with('/'))
        {
            return str.substr(1);
        }
        return str;
    };

    auto raw_path = metadata_path.serialize();

    if (raw_path.starts_with(table_location)
        && (raw_path.size() == table_location.size() || raw_path[table_location.size()] == '/'))
    {
        auto result = std::filesystem::path{table_root} / trim_forward_slash(raw_path.substr(table_location.size()));
        return result;
    }

    if (table_root.empty())
    {
        throw ::DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
            "IcebergPathResolver::resolve failed first branch. raw_path='{}', table_location='{}', table_root='{}'",
            raw_path, table_location, table_root);
    }


    auto pos = raw_path.find(table_root);
    /// Valid situation when data and metadata files are stored in different directories.
    if (pos == std::string::npos)
    {
        /// connection://bucket
        auto prefix = table_location.substr(0, table_location.size() - table_root.size());
        if (raw_path.size() < prefix.size())
        {
            throw ::DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "IcebergPathResolver::resolve failed in the second branch. raw_path='{}', table_location='{}', table_root='{}'",
                raw_path,
                table_location,
                table_root);
        }
        return std::string{raw_path.substr(prefix.size())};
    }

    size_t good_pos = std::string::npos;
    while (pos != std::string::npos)
    {
        auto potential_position = pos + table_root.size();
        if (((potential_position + 6 <= raw_path.size()) && (std::string_view(raw_path.data() + potential_position, 6) == "/data/"))
            || ((potential_position + 10 <= raw_path.size())
                && (std::string_view(raw_path.data() + potential_position, 10) == "/metadata/")))
        {
            good_pos = pos;
            break;
        }
        size_t new_pos = raw_path.find(table_root, pos + 1);
        if (new_pos == std::string::npos)
        {
            break;
        }
        pos = new_pos;
    }


    if (good_pos != std::string::npos)
    {
        return std::string{raw_path.substr(good_pos)};
    }
    else if (pos != std::string::npos)
    {
        return std::string{raw_path.substr(pos)};
    }
    else
    {
        throw ::DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Expected to find '{}' in data path: '{}'", table_root, raw_path);
    }
}
}
