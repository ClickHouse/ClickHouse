#pragma once

#include <Core/Block.h>
#include <DataStreams/IRowInputStream.h>


namespace DB
{

class ReadBuffer;

/** Поток для ввода данных в формате csv.
  * Не соответствует https://tools.ietf.org/html/rfc4180 потому что пропускает пробелы и табы между значениями.
  */
class CSVRowInputStream : public IRowInputStream
{
public:
    /** with_names - в первой строке заголовок с именами столбцов
      * with_types - на следующей строке заголовок с именами типов
      */
    CSVRowInputStream(ReadBuffer & istr_, const Block & sample_, const char delimiter_, bool with_names_ = false, bool with_types_ = false);

    bool read(Block & block) override;
    void readPrefix() override;
    bool allowSyncAfterError() const override { return true; };
    void syncAfterError() override;

    std::string getDiagnosticInfo() override;

private:
    ReadBuffer & istr;
    const Block sample;
    const char delimiter;
    bool with_names;
    bool with_types;
    DataTypes data_types;

    /// Для удобной диагностики в случае ошибки.

    size_t row_num = 0;

    /// Сколько байт было считано, не считая тех, что ещё в буфере.
    size_t bytes_read_at_start_of_buffer_on_current_row = 0;
    size_t bytes_read_at_start_of_buffer_on_prev_row = 0;

    char * pos_of_current_row = nullptr;
    char * pos_of_prev_row = nullptr;

    void updateDiagnosticInfo();

    bool parseRowAndPrintDiagnosticInfo(Block & block,
        WriteBuffer & out, size_t max_length_of_column_name, size_t max_length_of_data_type_name);
};

}
