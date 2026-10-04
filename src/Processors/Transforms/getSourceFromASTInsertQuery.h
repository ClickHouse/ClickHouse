#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferWrapperBase.h>
#include <IO/SnappyMode.h>
#include <base/defines.h>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
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
/// `snappy_mode` selects the snappy framing used when the data (e.g. `INSERT ... FROM INFILE`) is snappy-compressed.
std::unique_ptr<ReadBuffer> getReadBufferFromASTInsertQuery(const ASTPtr & ast, SnappyMode snappy_mode = SnappyMode::Basic);

/// For diagnostics only. Infers the structure of `data` in the given format (if the format supports
/// schema inference) and compares it with `expected_header`. Returns a human-readable explanation of
/// the mismatch, or an empty string when the structures correspond or nothing can be inferred. A
/// structure mismatch between the inserted data and the destination is a common and confusing cause
/// of parse errors (see https://github.com/ClickHouse/ClickHouse/issues/110622).
/// `rows_reached_by_parser` is the 1-based number of the row the parser had reached when it threw
/// (see IInputFormat::getRowsReachedOnParseError); when known, inference samples only that many rows,
/// so a row the parser never reached cannot contaminate the diagnosis of an earlier failure.
/// `data_is_truncated` tells that `data` is a bounded prefix of a longer payload (e.g. captured by a
/// PrefixCapturingReadBuffer that hit its cap). Inference treats the end of the data as the end of a
/// row, so a row cut off by such a bound could masquerade as a structure mismatch; when the flag is set
/// and inference had to consume the sample up to the cut to reach its row bound, the diagnostic is
/// suppressed rather than trusted.
String getInsertDataSchemaMismatchDescription(
    std::string_view data,
    const String & format_name,
    const Block & expected_header,
    const ContextPtr & context,
    std::optional<size_t> rows_reached_by_parser = std::nullopt,
    bool data_is_truncated = false);

/// Same as getInsertDataSchemaMismatchDescription, but reads a bounded prefix of the data from a file,
/// decompressing it the same way `INSERT ... FROM INFILE` itself would. Used for the client-side INFILE
/// path, where the input format is created deep inside a `StorageFile` pipeline and a lazy provider
/// cannot be attached to it directly.
String getInsertDataSchemaMismatchDescriptionFromFile(
    const String & file_path,
    const String & compression_method,
    const String & format_name,
    const Block & expected_header,
    const ContextPtr & context,
    std::optional<size_t> rows_reached_by_parser = std::nullopt);

/// Extracts the 1-based number of the row the parser had reached from a parse-error message, i.e. the
/// `(at row N)` part `IRowInputFormat` appends before rethrowing. Returns nullopt when the message does
/// not carry it. Used by the `INSERT ... FROM INFILE` path, where the input format lives deep inside a
/// `StorageFile` pipeline and cannot be asked for IInputFormat::getRowsReachedOnParseError directly.
/// Note that under parallel parsing the number is local to a chunk, so it is not a valid global bound.
std::optional<size_t> getRowsReachedFromParseErrorMessage(std::string_view message);

/// The bound for the prefix of a streamed (network / HTTP body / stdin) insert captured eagerly for the
/// parse-error diagnostic above. Unlike the inline and INFILE paths, which re-read the data only on the
/// error path, a streamed insert is consumed while parsing and cannot be re-read, so the prefix is
/// captured on every insert, including the ones that succeed. The full schema-inference sampling bound
/// (`input_format_max_bytes_to_read_for_schema_inference`, 32 MiB by default) would add a large copy to
/// the hot path for a best-effort error message; a small prefix is enough to infer the structure (column
/// count and types), so the capture is capped at a much smaller dedicated size. A row cut off by the cap
/// cannot masquerade as a structure mismatch: the diagnostic is suppressed when the sampled rows do not
/// lie whole within the captured prefix (see the `data_is_truncated` parameter of
/// getInsertDataSchemaMismatchDescription).
size_t getInsertDataPrefixCaptureLimitForDiagnostic(const ContextPtr & context);

/// Attaches getInsertDataSchemaMismatchDescription as a lazy diagnostic to the input format that
/// reads the inline data of an INSERT query: if parsing fails with a parse error, the resulting
/// explanation (if any) is appended to the exception message.
void setInsertSchemaMismatchDiagnostic(
    IInputFormat & format,
    const ASTPtr & ast,
    const String & format_name,
    const Block & expected_header,
    const ContextPtr & context);

/// A read buffer decorator that additionally captures a bounded prefix of the bytes read through it.
/// Used to make the parse-error diagnostic above available for data that comes from a source which
/// cannot be re-read once consumed and has no backing ASTInsertQuery::data (e.g. a client reading the
/// data of an INSERT from stdin, separately from the query text).
class PrefixCapturingReadBuffer : public ReadBuffer, public ReadBufferWrapperBase
{
public:
    PrefixCapturingReadBuffer(ReadBuffer & in_, size_t max_bytes_to_capture_);

    /// The captured prefix together with whether more bytes streamed through than the cap allowed to
    /// capture, i.e. the prefix is a truncated view of the data (see the `data_is_truncated` parameter
    /// of getInsertDataSchemaMismatchDescription).
    struct CapturedPrefix
    {
        String data;
        bool truncated = false;
    };

    /// Returns a snapshot of the capture. `ParallelParsingInputFormat` reads through this buffer in its
    /// segmentation thread while the parse-error diagnostic reads the capture from the thread that
    /// handles the exception, so the capture is taken under a lock and copied out: a `string_view` into
    /// the growing string would be read while another thread appends to it.
    CapturedPrefix getCapturedPrefix() const;

    /// Keep diagnostics that inspect the buffer chain (e.g. the file name added to error messages)
    /// working as if the wrapper were not there.
    const ReadBuffer & getWrappedReadBuffer() const override { return in; }

    /// Forward readiness polling to the wrapped buffer so streaming reads keep working through the
    /// wrapper — in particular the timeout-based partial flush of an HTTP insert
    /// (`input_format_connection_handling` + `input_format_max_block_wait_ms`), which relies on
    /// `poll` returning `false` when no data has arrived yet. The base `ReadBuffer::poll` always
    /// returns `true`, so without this the flush would never fire and the read would block.
    bool poll(size_t timeout_microseconds) override;

private:
    bool nextImpl() override;
    void captureFromCurrentBuffer();

    ReadBuffer & in;
    size_t max_bytes_to_capture;

    /// Guards the capture below: it is appended to by whichever thread reads through this buffer (the
    /// segmentation thread with `ParallelParsingInputFormat`) and read on the error path.
    mutable std::mutex capture_mutex;
    String captured TSA_GUARDED_BY(capture_mutex);
    bool prefix_truncated TSA_GUARDED_BY(capture_mutex) = false;
};

}
