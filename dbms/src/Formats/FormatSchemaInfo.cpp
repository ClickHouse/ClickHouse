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

FormatSchemaInfo::FormatSchemaInfo(const Context & context, const String & schema_file_extension, bool schema_required)
{
    String format_schema = context.getSettingsRef().format_schema.toString();
    if (format_schema.empty())
    {
        if (schema_required)
        {
            throw Exception(
                "Format schema requires the 'format_schema' setting to have the 'schema_file:message_name' format"
                    + (schema_file_extension.empty() ? "" : "e.g. 'schema." + schema_file_extension + ":Message'"),
                ErrorCodes::BAD_ARGUMENTS);
        }
        return;
    }

    size_t colon_pos = format_schema.find(':');
    Poco::Path path;
    if ((colon_pos == String::npos) || (colon_pos == 0) || (colon_pos == format_schema.length() - 1)
        || path.assign(format_schema.substr(0, colon_pos)).getFileName().empty())
    {
        throw Exception(
            "Format schema requires the 'format_schema' setting to have the 'schema_file:message_name' format"
                + (schema_file_extension.empty() ? "" : "e.g. 'schema." + schema_file_extension + ":Message'") + ". Got '" + format_schema
                + "'",
            ErrorCodes::BAD_ARGUMENTS);
    }

    is_null = false;
    message_name = format_schema.substr(colon_pos + 1);

    auto default_schema_directory = [&context]()
    {
        static const String str = Poco::Path(context.getFormatSchemaPath()).makeAbsolute().makeDirectory().toString();
        return str;
    };
    auto is_server = [&context]()
    {
        return context.hasGlobalContext() && (context.getGlobalContext().getApplicationType() == Context::ApplicationType::SERVER);
    };

    if (path.getExtension().empty() && !schema_file_extension.empty())
        path.setExtension(schema_file_extension);

    if (path.isAbsolute())
    {
        if (is_server())
            throw Exception("Absolute path in the 'format_schema' setting is prohibited: " + path.toString(), ErrorCodes::BAD_ARGUMENTS);
        schema_path = path.getFileName();
        schema_directory = path.makeParent().toString();
    }
    else if (path.depth() >= 1 && path.directory(0) == "..")
    {
        if (is_server())
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
