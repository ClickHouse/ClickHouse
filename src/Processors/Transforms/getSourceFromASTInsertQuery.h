#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferWrapperBase.h>
#include <cstddef>
#include <memory>
#include <string_view>


namespace DB
{

class Pipe;

/// Prepares a input format, which produce data containing in INSERT query.
InputFormatPtr getInputFormatFromASTInsertQuery(
    const ASTPtr & ast,
    bool with_buffers,
    const Block & header,
    ContextPtr context,
    const ASTPtr & input_function);

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

/// Prepares a read buffer, that allows to read inlined data
/// from ASTInsertQuert directly, and from tail buffer, if it exists.
std::unique_ptr<ReadBuffer> getReadBufferFromASTInsertQuery(const ASTPtr & ast);

/// For diagnostics only. Infers the structure of `data` in the given format (if the format supports
/// schema inference) and compares it with `expected_header`. Returns a human-readable explanation of
/// the mismatch, or an empty string when the structures correspond or nothing can be inferred. A
/// structure mismatch between the inserted data and the destination is a common and confusing cause
/// of parse errors (see https://github.com/ClickHouse/ClickHouse/issues/110622).
String getInsertDataSchemaMismatchDescription(
    std::string_view data, const String & format_name, const Block & expected_header, const ContextPtr & context);

/// Attaches getInsertDataSchemaMismatchDescription as a lazy diagnostic to the input format that
/// reads the inline data of an INSERT query: if parsing fails with a parse error, the resulting
/// explanation (if any) is appended to the exception message.
void setInsertSchemaMismatchDiagnostic(
    IInputFormat & format, const ASTPtr & ast, const Block & expected_header, const ContextPtr & context);

/// A read buffer decorator that additionally captures a bounded prefix of the bytes read through it.
/// Used to make the parse-error diagnostic above available for data that comes from a source which
/// cannot be re-read once consumed and has no backing ASTInsertQuery::data (e.g. a client reading the
/// data of an INSERT from stdin, separately from the query text).
class PrefixCapturingReadBuffer : public ReadBuffer, public ReadBufferWrapperBase
{
public:
    PrefixCapturingReadBuffer(ReadBuffer & in_, size_t max_bytes_to_capture_);

    std::string_view getCapturedPrefix() const { return captured; }

    /// Keep diagnostics that inspect the buffer chain (e.g. the file name added to error messages)
    /// working as if the wrapper were not there.
    const ReadBuffer & getWrappedReadBuffer() const override { return in; }

private:
    bool nextImpl() override;
    void captureFromCurrentBuffer();

    ReadBuffer & in;
    size_t max_bytes_to_capture;
    String captured;
};

}
