#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IRowOutputStream.h>


namespace DB
{

/** Преобразует поток для записи данных по строкам в поток для записи данных по блокам.
  * Наример, для записи текстового дампа.
  */
class BlockOutputStreamFromRowOutputStream : public IBlockOutputStream
{
public:
    BlockOutputStreamFromRowOutputStream(RowOutputStreamPtr row_output_);
    void write(const Block & block) override;
    void writePrefix() override { row_output->writePrefix(); }
    void writeSuffix() override { row_output->writeSuffix(); }

    void flush() override { row_output->flush(); }

    void setRowsBeforeLimit(size_t rows_before_limit) override;
    void setTotals(const Block & totals) override;
    void setExtremes(const Block & extremes) override;
    void onProgress(const Progress & progress) override;

    String getContentType() const override { return row_output->getContentType(); }

private:
    RowOutputStreamPtr row_output;
    bool first_row;
};

}
