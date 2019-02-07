#pragma once

#include <Core/Types.h>

namespace DB
{
class Context;

/// Extracts information about where the format schema file is from passed context and keep it.
class FormatSchemaInfo
{
public:
    FormatSchemaInfo() = default;
    FormatSchemaInfo(const Context & context, const String & schema_file_extension = String(), bool schema_required = true);

    bool isNull() const { return is_null; }

    /// Returns path to the schema file.
    const String & schemaPath() const { return schema_path; }
    String absoluteSchemaPath() const { return schema_directory + schema_path; }

    /// Returns directory containing the schema file.
    const String & schemaDirectory() const { return schema_directory; }

    /// Returns name of the message type.
    const String & messageName() const { return message_name; }

private:
    bool is_null = true;
    String schema_path;
    String schema_directory;
    String message_name;
};

}
