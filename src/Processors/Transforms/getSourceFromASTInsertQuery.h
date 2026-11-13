#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IInputFormat.h>
#include <IO/SnappyMode.h>
#include <cstddef>
#include <functional>
#include <memory>
#include <string_view>


namespace DB
{

class Pipe;

/// Resolves the input format for an INSERT query, including `input_format` / `format` setting overrides.
String getInputFormatNameFromASTInsertQuery(const ASTPtr & ast, const ContextPtr & context);

/// Prepares a input format, which produce data containing in INSERT query.
InputFormatPtr getInputFormatFromASTInsertQuery(
    const ASTPtr & ast,
    bool with_buffers,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function);

/// Logs a warning for each skipped unknown field of the INSERT query `query_id`, once per field across all copies, at most 10 fields.
/// Empty if `input_format_warn_on_skipped_unknown_fields` is disabled.
std::function<void(std::string_view)> makeSkippedUnknownFieldWarningCallback(const ContextPtr & context, const String & format_name, const String & query_id);

/// Installs `makeSkippedUnknownFieldWarningCallback` on `format`.
void warnAboutSkippedUnknownFields(IInputFormat & format, const ContextPtr & context, const String & format_name, const String & query_id);

/// Prepares a pipe from input format got from ASTInsertQuery,
/// which produce data containing in INSERT query.
Pipe getSourceFromInputFormat(
    const ASTPtr & ast,
    InputFormatPtr format,
    ContextPtr context,
    const ASTPtr & input_function);

/// Prepares a pipe which produce data containing in INSERT query.
Pipe getSourceFromASTInsertQuery(
    const ASTPtr & ast,
    bool with_buffers,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function);

class ReadBuffer;

/// Prepares a read buffer, that allows to read inlined data
/// from ASTInsertQuert directly, and from tail buffer, if it exists.
/// `snappy_mode` selects the snappy framing used when the data (e.g. `INSERT ... FROM INFILE`) is snappy-compressed.
std::unique_ptr<ReadBuffer> getReadBufferFromASTInsertQuery(const ASTPtr & ast, SnappyMode snappy_mode = SnappyMode::Basic);

}
