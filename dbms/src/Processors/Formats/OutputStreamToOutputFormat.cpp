#include <Processors/Formats/OutputStreamToOutputFormat.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{

Block OutputStreamToOutputFormat::getHeader() const
{
    return output_format->getPort(IOutputFormat::PortKind::Main).getHeader();
}

void OutputStreamToOutputFormat::write(const Block & block)
{
    output_format->write(block);
}

void OutputStreamToOutputFormat::writePrefix() { output_format->doWritePrefix(); }
void OutputStreamToOutputFormat::writeSuffix() { output_format->doWriteSuffix(); }

void OutputStreamToOutputFormat::flush() { output_format->flush(); }

void OutputStreamToOutputFormat::setRowsBeforeLimit(size_t rows_before_limit)
{
    output_format->setRowsBeforeLimit(rows_before_limit);
}

void OutputStreamToOutputFormat::setTotals(const Block & totals)
{
    if (totals)
        output_format->setTotals(totals);
}

void OutputStreamToOutputFormat::setExtremes(const Block & extremes)
{
    if (extremes)
        output_format->setExtremes(extremes);
}

void OutputStreamToOutputFormat::onProgress(const Progress & progress) { output_format->onProgress(progress); }

std::string OutputStreamToOutputFormat::getContentType() const { return output_format->getContentType(); }

}
