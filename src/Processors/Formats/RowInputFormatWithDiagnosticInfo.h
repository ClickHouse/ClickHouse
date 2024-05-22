#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <IO/ReadBuffer.h>
#include <limits>


namespace DB
{

class RowInputFormatWithDiagnosticInfo : public IRowInputFormat
{
public:
    RowInputFormatWithDiagnosticInfo(const Block & header_, ReadBuffer & in_, const Params & params_);

    std::pair<String, String> getDiagnosticAndRawDataImpl(bool is_errors_record);
    String getDiagnosticInfo() override;
    std::pair<String, String> getDiagnosticAndRawData() override;

    void resetParser() override;

protected:
    void updateDiagnosticInfo();
    bool deserializeFieldAndPrintDiagnosticInfo(const String & col_name, const DataTypePtr & type, IColumn & column,
                                                WriteBuffer & out, size_t file_column);

    virtual bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out) = 0;
    virtual void tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column) = 0;
    virtual bool isGarbageAfterField(size_t after_input_pos_idx, ReadBuffer::Position pos) = 0;

private:
    /// How many bytes were read, not counting those still in the buffer.
    size_t bytes_read_at_start_of_buffer_on_current_row = 0;
    size_t bytes_read_at_start_of_buffer_on_prev_row = 0;

    size_t offset_of_current_row = std::numeric_limits<size_t>::max();
    size_t offset_of_prev_row = std::numeric_limits<size_t>::max();

    /// For alignment of diagnostic info.
    size_t max_length_of_column_name = 0;
    size_t max_length_of_data_type_name = 0;
};

}
