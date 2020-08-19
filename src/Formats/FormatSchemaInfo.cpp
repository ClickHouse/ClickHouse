#include <Formats/FormatSchemaInfo.h>
#include <Poco/Path.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


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

    Poco::Path path;
    if (require_message)
    {
        size_t colon_pos = format_schema.find(':');
        if ((colon_pos == String::npos) || (colon_pos == 0) || (colon_pos == format_schema.length() - 1)
            || path.assign(format_schema.substr(0, colon_pos)).makeFile().getFileName().empty())
        {
            throw Exception(
                    "Format schema requires the 'format_schema' setting to have the 'schema_file:message_name' format"
                    + (default_file_extension.empty() ? "" : ", e.g. 'schema." + default_file_extension + ":Message'") +
                    ". Got '" + format_schema
                    + "'",
                    ErrorCodes::BAD_ARGUMENTS);
        }

        message_name = format_schema.substr(colon_pos + 1);
    }
    else
        path.assign(format_schema).makeFile().getFileName();

    auto default_schema_directory = [&format_schema_path]()
    {
        static const String str = Poco::Path(format_schema_path).makeAbsolute().makeDirectory().toString();
        return str;
    };

    if (path.getExtension().empty() && !default_file_extension.empty())
        path.setExtension(default_file_extension);

    if (path.isAbsolute())
    {
        if (is_server)
            throw Exception("Absolute path in the 'format_schema' setting is prohibited: " + path.toString(), ErrorCodes::BAD_ARGUMENTS);
        schema_path = path.getFileName();
        schema_directory = path.makeParent().toString();
    }
    else if (path.depth() >= 1 && path.directory(0) == "..")
    {
        if (is_server)
            throw Exception(
                "Path in the 'format_schema' setting shouldn't go outside the 'format_schema_path' directory: " + path.toString(),
                ErrorCodes::BAD_ARGUMENTS);
        path = Poco::Path(default_schema_directory()).resolve(path).toString();
        schema_path = path.getFileName();
        schema_directory = path.makeParent().toString();
    }
    else
    {
        schema_path = path.toString();
        schema_directory = default_schema_directory();
    }
}

}
