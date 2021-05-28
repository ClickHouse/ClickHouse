#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>


namespace DB
{

class NativeInputFormatFromNativeBlockInputStream : public IInputFormat
{
public:
    NativeInputFormatFromNativeBlockInputStream(const Block & header, ReadBuffer & in_)
        : IInputFormat(header, in_)
        , stream(std::make_shared<NativeBlockInputStream>(in, header, 0))
    {
    }

    String getName() const override { return "NativeInputFormatFromNativeBlockInputStream"; }

protected:
    void resetParser() override
    {
        IInputFormat::resetParser();
        stream->resetParser();
        read_prefix = false;
        read_suffix = false;
    }


    Chunk generate() override
    {
        /// TODO: do something with totals and extremes.

        if (!read_prefix)
        {
            stream->readPrefix();
            read_prefix = true;
        }

        auto block = stream->read();
        if (!block)
        {
            if (!read_suffix)
            {
                stream->readSuffix();
                read_suffix = true;
            }

            return Chunk();
        }

        assertBlocksHaveEqualStructure(getPort().getHeader(), block, getName());
        block.checkNumberOfRows();

        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

private:
    std::shared_ptr<NativeBlockInputStream> stream;
    bool read_prefix = false;
    bool read_suffix = false;
};


class NativeOutputFormatFromNativeBlockOutputStream : public IOutputFormat
{
public:
    NativeOutputFormatFromNativeBlockOutputStream(const Block & header, WriteBuffer & out_)
            : IOutputFormat(header, out_)
            , stream(std::make_shared<NativeBlockOutputStream>(out, 0, header))
    {
    }

    String getName() const override { return "NativeOutputFormatFromNativeBlockOutputStream"; }

    void setRowsBeforeLimit(size_t rows_before_limit) override
    {
        stream->setRowsBeforeLimit(rows_before_limit);
    }

    void onProgress(const Progress & progress) override
    {
        stream->onProgress(progress);
    }

    std::string getContentType() const override
    {
        return stream->getContentType();
    }

protected:
    void consume(Chunk chunk) override
    {
        writePrefixIfNot();

        if (chunk)
        {

            auto block = getPort(PortKind::Main).getHeader();
            block.setColumns(chunk.detachColumns());
            stream->write(block);
        }
    }

    void consumeTotals(Chunk chunk) override
    {
        writePrefixIfNot();

        auto block = getPort(PortKind::Totals).getHeader();
        block.setColumns(chunk.detachColumns());
        stream->setTotals(block);
    }

    void consumeExtremes(Chunk chunk) override
    {
        writePrefixIfNot();

        auto block = getPort(PortKind::Extremes).getHeader();
        block.setColumns(chunk.detachColumns());
        stream->setExtremes(block);
    }

    void finalize() override
    {
        writePrefixIfNot();
        writeSuffixIfNot();
    }

private:
    std::shared_ptr<NativeBlockOutputStream> stream;
    bool prefix_written = false;
    bool suffix_written = false;

    void writePrefixIfNot()
    {
        if (!prefix_written)
            stream->writePrefix();

        prefix_written = true;
    }

    void writeSuffixIfNot()
    {
        if (!suffix_written)
            stream->writeSuffix();

        suffix_written = true;
    }
};

void registerInputFormatProcessorNative(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("Native", [](
        ReadBuffer & buf,
        const Block & sample,
        const RowInputFormatParams &,
        const FormatSettings &)
    {
        return std::make_shared<NativeInputFormatFromNativeBlockInputStream>(sample, buf);
    });
}

void registerOutputFormatProcessorNative(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("Native", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback,
        const FormatSettings &)
    {
        return std::make_shared<NativeOutputFormatFromNativeBlockOutputStream>(sample, buf);
    });
}

}
