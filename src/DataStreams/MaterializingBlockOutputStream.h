#pragma once

#include <DataStreams/materializeBlock.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Converts columns-constants to full columns ("materializes" them).
  */
class MaterializingBlockOutputStream : public IBlockOutputStream
{
public:
    MaterializingBlockOutputStream(const BlockOutputStreamPtr & output_, const Block & header_)
        : output{output_}, header(header_) {}

    Block getHeader() const                           override { return header; }
    void write(const Block & block)                   override { output->write(materializeBlock(block)); }
    void flush()                                      override { output->flush(); }
    void writePrefix()                                override { output->writePrefix(); }
    void writeSuffix()                                override { output->writeSuffix(); }
    void setRowsBeforeLimit(size_t rows_before_limit) override { output->setRowsBeforeLimit(rows_before_limit); }
    void setTotals(const Block & totals)              override { output->setTotals(materializeBlock(totals)); }
    void setExtremes(const Block & extremes)          override { output->setExtremes(materializeBlock(extremes)); }
    void onProgress(const Progress & progress)        override { output->onProgress(progress); }
    String getContentType() const                     override { return output->getContentType(); }

private:
    BlockOutputStreamPtr output;
    Block header;
};

}
