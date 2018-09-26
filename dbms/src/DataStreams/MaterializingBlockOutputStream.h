/* Some modifications Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
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
    MaterializingBlockOutputStream(const BlockOutputStreamPtr & output, const Block & header)
        : output{output}, header(header) {}

    Block getHeader() const                           override { return header; }
    void write(const Block & block)                   override { output->write(materializeBlock(block)); }
    void flush()                                      override { output->flush(); }
    void writePrefix()                                override { output->writePrefix(); }
    void writeSuffix()                                override { output->writeSuffix(); }
    void setSampleBlock(const Block & sample)         override { output->setSampleBlock(sample); }
    void setRowsBeforeLimit(size_t rows_before_limit) override { output->setRowsBeforeLimit(rows_before_limit); }
    void setTotals(const Block & totals)              override { output->setTotals(materializeBlock(totals)); }
    void setExtremes(const Block & extremes)          override { output->setExtremes(materializeBlock(extremes)); }
    void onProgress(const Progress & progress)        override { output->onProgress(progress); }
    void onHeartbeat(const Heartbeat & heartbeat)     override { output->onHeartbeat(heartbeat); }
    String getContentType() const                     override { return output->getContentType(); }

private:
    BlockOutputStreamPtr output;
    Block header;
};

}
