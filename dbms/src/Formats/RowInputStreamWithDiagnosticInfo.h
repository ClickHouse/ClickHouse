#pragma once

#include <Core/Block.h>
#include <Formats/IRowInputStream.h>
#include <IO/ReadBuffer.h>


namespace DB
{

class RowInputStreamWithDiagnosticInfo : public IRowInputStream
{
public:
    RowInputStreamWithDiagnosticInfo(ReadBuffer & istr_, const Block & header_);

    String getDiagnosticInfo() override;

protected:
    void updateDiagnosticInfo();
    bool deserializeFieldAndPrintDiagnosticInfo(const String & col_name, const DataTypePtr & type, IColumn & column,
                                                WriteBuffer & out, size_t input_position);
    String alignedName(const String & name, size_t max_length) const;

    virtual bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out) = 0;
    virtual void tryDeserializeFiled(const DataTypePtr & type, IColumn & column, size_t input_position, ReadBuffer::Position & prev_pos,
                                     ReadBuffer::Position & curr_pos) = 0;
    virtual bool isGarbageAfterField(size_t after_input_pos_idx, ReadBuffer::Position pos) = 0;

    ReadBuffer & istr;
    Block header;

    /// For convenient diagnostics in case of an error.
    size_t row_num = 0;

private:
    /// How many bytes were read, not counting those still in the buffer.
    size_t bytes_read_at_start_of_buffer_on_current_row = 0;
    size_t bytes_read_at_start_of_buffer_on_prev_row = 0;

    char * pos_of_current_row = nullptr;
    char * pos_of_prev_row = nullptr;

    /// For alignment of diagnostic info.
    size_t max_length_of_column_name = 0;
    size_t max_length_of_data_type_name = 0;
};

}
