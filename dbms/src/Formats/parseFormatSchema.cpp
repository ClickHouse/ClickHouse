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
    if ((colon_pos == String::npos) || (colon_pos == 0) || (colon_pos == format_schema.length() - 1))
    {
        throw Exception(
            "Format schema requires the 'format_schema' setting to have the 'schema_file:message_name' format"
                + (schema_file_extension.empty() ? "" : "e.g. 'schema." + schema_file_extension + ":Message'") + ". Got '" + format_schema
                + "'",
            ErrorCodes::BAD_ARGUMENTS);
    }

    Poco::Path path(format_schema.substr(0, colon_pos));
    if (context.hasGlobalContext() && (context.getGlobalContext().getApplicationType() == Context::ApplicationType::SERVER))
    {
        if (path.isAbsolute())
            throw Exception("Absolute path in the 'format_schema' setting is prohibited: " + path.toString(), ErrorCodes::BAD_ARGUMENTS);

        if (path.depth() >= 1 && path.directory(0) == "..")
            throw Exception(
                "Path in the 'format_schema' setting shouldn't go outside the 'format_schema_path' directory: " + path.toString(),
                ErrorCodes::BAD_ARGUMENTS);
    }

    if (path.getExtension().empty() && !schema_file_extension.empty())
        path.setExtension(schema_file_extension);

    schema_path = path.toString();
    schema_directory = context.getFormatSchemaPath();
    message_name = format_schema.substr(colon_pos + 1);
    is_null = false;
}

}
