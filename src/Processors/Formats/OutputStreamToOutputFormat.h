#pragma once
#include <DataStreams/IBlockOutputStream.h>

namespace DB
{


class IOutputFormat;

using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

/// Wrapper. Implements IBlockOutputStream interface using IOutputFormat object.
class OutputStreamToOutputFormat : public IBlockOutputStream
{
public:
    explicit OutputStreamToOutputFormat(OutputFormatPtr output_format_) : output_format(std::move(output_format_)) {}

    Block getHeader() const override;

    void write(const Block & block) override;

    void writePrefix() override;
    void writeSuffix() override;

    void flush() override;

    void setRowsBeforeLimit(size_t rows_before_limit) override;
    void setTotals(const Block & totals) override;
    void setExtremes(const Block & extremes) override;

    void onProgress(const Progress & progress) override;

    std::string getContentType() const override;

private:
    OutputFormatPtr output_format;
};

}
