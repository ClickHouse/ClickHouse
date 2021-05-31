#include <Formats/FormatSchemaInfo.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <filesystem>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace fs = std::filesystem;

namespace
{
    String getFormatSchemaDefaultFileExtension(const String & format)
    {
        if (format == "Protobuf")
            return "proto";
        else if (format == "CapnProto")
            return "capnp";
        else
            return "";
    }
}


FormatSchemaInfo::FormatSchemaInfo(const String & format_schema, const String & format, bool require_message, bool is_server, const std::string & format_schema_path)
{
    if (format_schema.empty())
        throw Exception(
            "The format " + format + " requires a schema. The corresponding setting should be set", ErrorCodes::BAD_ARGUMENTS);

    String default_file_extension = getFormatSchemaDefaultFileExtension(format);

    fs::path path;
    if (require_message)
    {
        size_t colon_pos = format_schema.find(':');
        if ((colon_pos == String::npos) || (colon_pos == 0) || (colon_pos == format_schema.length() - 1))
        {
            throw Exception(
                    "Format schema requires the 'format_schema' setting to have the 'schema_file:message_name' format"
                    + (default_file_extension.empty() ? "" : ", e.g. 'schema." + default_file_extension + ":Message'") +
                    ". Got '" + format_schema + "'", ErrorCodes::BAD_ARGUMENTS);
        }
        else
        {
            path = fs::path(format_schema.substr(0, colon_pos));
            String filename = path.has_filename() ? path.filename() : path.parent_path().filename();
            if (filename.empty())
                throw Exception(
                    "Format schema requires the 'format_schema' setting to have the 'schema_file:message_name' format"
                    + (default_file_extension.empty() ? "" : ", e.g. 'schema." + default_file_extension + ":Message'") +
                    ". Got '" + format_schema + "'", ErrorCodes::BAD_ARGUMENTS);
        }
        message_name = format_schema.substr(colon_pos + 1);
    }
    else
    {
        path = fs::path(format_schema);
        if (!path.has_filename())
            path = path.parent_path() / "";
    }

    auto default_schema_directory = [&format_schema_path]()
    {
        static const String str = fs::canonical(format_schema_path) / "";
        return str;
    };

    if (!path.has_extension() && !default_file_extension.empty())
        path = path.parent_path() / (path.stem().string() + '.' + default_file_extension);

    fs::path default_schema_directory_path(default_schema_directory());
    if (path.is_absolute())
    {
        if (is_server)
            throw Exception("Absolute path in the 'format_schema' setting is prohibited: " + path.string(), ErrorCodes::BAD_ARGUMENTS);
        schema_path = path.filename();
        schema_directory = path.parent_path() / "";
    }
    else if (path.has_parent_path() && !fs::weakly_canonical(default_schema_directory_path / path).string().starts_with(fs::weakly_canonical(default_schema_directory_path).string()))
    {
        if (is_server)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Path in the 'format_schema' setting shouldn't go outside the 'format_schema_path' directory: {} ({} not in {})",
                            path.string());
        path = default_schema_directory_path / path;
        schema_path = path.filename();
        schema_directory = path.parent_path() / "";
    }
    else
    {
        schema_path = path;
        schema_directory = default_schema_directory();
    }
}

}
