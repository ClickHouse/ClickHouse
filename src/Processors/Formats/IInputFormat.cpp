#include <optional>
#include <Processors/Formats/IInputFormat.h>
#include <Formats/ParseError.h>
#include <IO/ReadBuffer.h>
#include <IO/WithFileName.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <IO/VarInt.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace FailPoints
{
    extern const char insert_schema_mismatch_diagnostic_fail[];
    extern const char insert_schema_mismatch_diagnostic_attach_fail[];
}

bool isInsertDataErrorWorthExplaining(int code)
{
    return isParseError(code) || code == ErrorCodes::CANNOT_PARSE_TEXT;
}

ChunkInfoRowNumbers::ChunkInfoRowNumbers(size_t row_num_offset_, std::optional<IColumnFilter> applied_filter_)
    : row_num_offset(row_num_offset_), applied_filter(std::move(applied_filter_)) { }

ChunkInfoRowNumbers::Ptr ChunkInfoRowNumbers::clone() const
{
    auto res = std::make_shared<ChunkInfoRowNumbers>(row_num_offset);
    if (applied_filter.has_value())
        res->applied_filter.emplace(applied_filter->begin(), applied_filter->end());
    return res;
}

IInputFormat::IInputFormat(SharedHeader header, ReadBuffer * in_) : ISource(std::move(header)), in(in_)
{
    column_mapping = std::make_shared<ColumnMapping>();
}

Chunk IInputFormat::generate()
{
    try
    {
        Chunk res = read();
        return res;
    }
    catch (Exception & e)
    {
        auto file_name = getFileNameFromReadBuffer(getReadBuffer());
        if (!file_name.empty())
            e.addMessage(fmt::format("(in file/uri {})", file_name));

        /// For a parse or conversion error, attach the schema-mismatch explanation if one is available.
        if (parse_error_diagnostic_provider && isInsertDataErrorWorthExplaining(e.code()))
        {
            try
            {
                /// Stands in for a failure while the diagnostic is prepared, e.g. exceeding the memory
                /// limit when a provider copies the captured data.
                fiu_do_on(FailPoints::insert_schema_mismatch_diagnostic_fail,
                {
                    throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Failpoint {} is triggered", FailPoints::insert_schema_mismatch_diagnostic_fail);
                });
                String diagnostic = parse_error_diagnostic_provider(getRowsReachedOnParseError());
                if (!diagnostic.empty())
                {
                    /// Stands in for a failure while a prepared explanation is attached, e.g. exceeding
                    /// the memory limit when `addMessage` copies it into the message of the exception.
                    fiu_do_on(FailPoints::insert_schema_mismatch_diagnostic_attach_fail,
                    {
                        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Failpoint {} is triggered", FailPoints::insert_schema_mismatch_diagnostic_attach_fail);
                    });
                    e.addMessage(diagnostic);
                }
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// The explanation is an optional addition to the exception being handled. A failure
                /// anywhere in preparing it, including in the provider before the explanation itself
                /// is computed (e.g. copying the captured data under the query memory limit), or in
                /// attaching it to the exception (`addMessage` allocates as well), must not replace
                /// the original exception, so it is Ok to ignore it and omit the explanation.
            }
        }

        throw;
    }
}

void IInputFormat::resetParser()
{
    if (in)
        in->ignoreAll();

    // those are protected attributes from ISource (I didn't want to propagate resetParser up there)
    finished = false;
    got_exception = false;

    getPort().getInputPort().reopen();
}

void IInputFormat::setReadBuffer(ReadBuffer & in_)
{
    in = &in_;
}

Chunk IInputFormat::getChunkForCount(size_t rows)
{
    const auto & header = getPort().getHeader();
    return cloneConstWithDefault(Chunk{header.getColumns(), 0}, rows);
}

void IInputFormat::resetOwnedBuffers()
{
    owned_buffers.clear();
}

void IInputFormat::onFinish()
{
    resetReadBuffer();
}

void IInputFormat::setBucketsToRead(const FileBucketInfoPtr & /*buckets_to_read*/)
{
}

}
