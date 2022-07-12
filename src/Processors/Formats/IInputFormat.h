#pragma once

#include <Processors/ISource.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Formats/ColumnMapping.h>


namespace DB
{

using ColumnMappingPtr = std::shared_ptr<ColumnMapping>;

/** Input format is a source, that reads data from ReadBuffer.
  */
class IInputFormat : public ISource
{
protected:

    /// Skip GCC warning: ‘maybe_unused’ attribute ignored
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"

    ReadBuffer * in [[maybe_unused]];

#pragma GCC diagnostic pop

public:
    IInputFormat(Block header, ReadBuffer & in_);

    /** In some usecase (hello Kafka) we need to read a lot of tiny streams in exactly the same format.
     * The recreating of parser for each small stream takes too long, so we introduce a method
     * resetParser() which allow to reset the state of parser to continue reading of
     * source stream without recreating that.
     * That should be called after current buffer was fully read.
     */
    virtual void resetParser();

    virtual void setReadBuffer(ReadBuffer & in_);
    const ReadBuffer & getReadBuffer() const { return *in; }

    virtual const BlockMissingValues & getMissingValues() const
    {
        static const BlockMissingValues none;
        return none;
    }

    /// Must be called from ParallelParsingInputFormat after readSuffix
    ColumnMappingPtr getColumnMapping() const { return column_mapping; }
    /// Must be called from ParallelParsingInputFormat before readPrefix
    void setColumnMapping(ColumnMappingPtr column_mapping_) { column_mapping = column_mapping_; }

    size_t getCurrentUnitNumber() const { return current_unit_number; }
    void setCurrentUnitNumber(size_t current_unit_number_) { current_unit_number = current_unit_number_; }

    void addBuffer(std::unique_ptr<ReadBuffer> buffer) { owned_buffers.emplace_back(std::move(buffer)); }

protected:
    ColumnMappingPtr column_mapping{};

private:
    /// Number of currently parsed chunk (if parallel parsing is enabled)
    size_t current_unit_number = 0;

    std::vector<std::unique_ptr<ReadBuffer>> owned_buffers;
};

using InputFormatPtr = std::shared_ptr<IInputFormat>;

}
