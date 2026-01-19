#include <Formats/BuffersReader.h>
#include <Formats/BuffersWriter.h>
#include <Formats/FormatFactory.h>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{


class BuffersInputFormat final : public IInputFormat
{
public:
    BuffersInputFormat(ReadBuffer & buf, SharedHeader header_, const FormatSettings & settings_)
        : IInputFormat(header_, &buf)
        , reader(std::make_unique<BuffersReader>(buf, *header_, settings_))
        , header(header_)
        , settings(settings_)
    {
    }

    String getName() const override { return "Buffers"; }

    void resetParser() override { IInputFormat::resetParser(); }

    Chunk read() override
    {
        size_t block_start = getDataOffsetMaybeCompressed(*in);
        auto block = reader->read();
        approx_bytes_read_for_chunk = getDataOffsetMaybeCompressed(*in) - block_start;

        if (block.empty())
            return {};

        assertBlocksHaveEqualStructure(getPort().getHeader(), block, getName());
        block.checkNumberOfRows();

        size_t num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    void setReadBuffer(ReadBuffer & in_) override
    {
        reader = std::make_unique<BuffersReader>(in_, *header, settings);
        IInputFormat::setReadBuffer(in_);
    }

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

private:
    std::unique_ptr<BuffersReader> reader;
    SharedHeader header;
    const FormatSettings settings;
    size_t approx_bytes_read_for_chunk = 0;
};


class BuffersOutputFormat final : public IOutputFormat
{
public:
    BuffersOutputFormat(WriteBuffer & buf, SharedHeader header, const FormatSettings & settings)
        : IOutputFormat(header, buf)
        , writer(buf, header, settings)
    {
    }

    String getName() const override { return "Buffers"; }

protected:
    void consume(Chunk chunk) override
    {
        if (!chunk)
            return;

        auto block = getPort(PortKind::Main).getHeader();
        block.setColumns(chunk.detachColumns());
        writer.write(block);
    }

private:
    BuffersWriter writer;
};

void registerInputFormatBuffers(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Buffers",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings & settings)
        { return std::make_shared<BuffersInputFormat>(buf, std::make_shared<const Block>(sample), settings); });
}

void registerOutputFormatBuffers(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Buffers",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr /*format_filter_info*/)
        { return std::make_shared<BuffersOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });

    factory.markOutputFormatNotTTYFriendly("Buffers");
    factory.setContentType("Buffers", "application/octet-stream");
}

}
