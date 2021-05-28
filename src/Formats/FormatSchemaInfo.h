#pragma once

#include <Core/Types.h>

namespace DB
{
class Context;

/// Extracts information about where the format schema file is from passed context and keep it.
class FormatSchemaInfo
{
public:
    FormatSchemaInfo(const String & format_schema, const String & format, bool require_message, bool is_server, const std::string & format_schema_path);

    /// Returns path to the schema file.
    const String & schemaPath() const { return schema_path; }
    String absoluteSchemaPath() const { return schema_directory + schema_path; }

    /// Returns directory containing the schema file.
    const String & schemaDirectory() const { return schema_directory; }

    /// Returns name of the message type.
    const String & messageName() const { return message_name; }

private:
    String schema_path;
    String schema_directory;
    String message_name;
};

}
