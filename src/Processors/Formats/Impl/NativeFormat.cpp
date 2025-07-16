#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Transforms/AggregatingTransform.h>


namespace DB
{


class NativeInputFormat final : public IInputFormat
{
public:
    NativeInputFormat(ReadBuffer & buf, const Block & header_, const FormatSettings & settings_)
        : IInputFormat(header_, &buf)
        , reader(std::make_unique<NativeReader>(
              buf,
              header_,
              0,
              settings_,
              settings_.defaults_for_omitted_fields ? &block_missing_values : nullptr))
        , header(header_)
        , block_missing_values(header.columns())
        , settings(settings_)
        {
        }

    String getName() const override { return "Native"; }

    void resetParser() override
    {
        IInputFormat::resetParser();
        reader->resetParser();
    }

    Chunk read() override
    {
        block_missing_values.clear();
        size_t block_start = getDataOffsetMaybeCompressed(*in);
        auto block = reader->read();
        approx_bytes_read_for_chunk = getDataOffsetMaybeCompressed(*in) - block_start;

        if (!block)
            return {};

        assertBlocksHaveEqualStructure(getPort().getHeader(), block, getName());
        block.checkNumberOfRows();

        size_t num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    void setReadBuffer(ReadBuffer & in_) override
    {
        reader = std::make_unique<NativeReader>(in_, header, 0, settings, settings.defaults_for_omitted_fields ? &block_missing_values : nullptr);
        IInputFormat::setReadBuffer(in_);
    }

    const BlockMissingValues * getMissingValues() const override { return &block_missing_values; }

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

private:
    std::unique_ptr<NativeReader> reader;
    Block header;
    BlockMissingValues block_missing_values;
    const FormatSettings settings;
    size_t approx_bytes_read_for_chunk = 0;
};

class NativeOutputFormat final : public IOutputFormat
{
public:
    NativeOutputFormat(WriteBuffer & buf, const Block & header, const FormatSettings & settings, UInt64 client_protocol_version = 0)
        : IOutputFormat(header, buf)
        , writer(buf, client_protocol_version, header, settings)
    {
    }

    String getName() const override { return "Native"; }

protected:
    void consume(Chunk chunk) override
    {
        if (chunk)
        {
            auto block = getPort(PortKind::Main).getHeader();
            block.setColumns(chunk.detachColumns());
            writer.write(block);
        }
    }

private:
    NativeWriter writer;
};

class NativeSchemaReader : public ISchemaReader
{
public:
    explicit NativeSchemaReader(ReadBuffer & in_, const FormatSettings & settings_) : ISchemaReader(in_), settings(settings_) {}

    NamesAndTypesList readSchema() override
    {
        auto reader = NativeReader(in, 0, settings);
        auto block = reader.read();
        return block.getNamesAndTypesList();
    }

private:
    const FormatSettings settings;
};


void registerInputFormatNative(FormatFactory & factory)
{
    factory.registerInputFormat("Native", [](
        ReadBuffer & buf,
        const Block & sample,
        const RowInputFormatParams &,
        const FormatSettings & settings)
    {
        return std::make_shared<NativeInputFormat>(buf, sample, settings);
    });
    factory.markFormatSupportsSubsetOfColumns("Native");
}

void registerOutputFormatNative(FormatFactory & factory)
{
    factory.registerOutputFormat("Native", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & settings)
    {
        return std::make_shared<NativeOutputFormat>(buf, sample, settings, settings.client_protocol_version);
    });
    factory.markOutputFormatNotTTYFriendly("Native");
    factory.setContentType("Native", "application/octet-stream");
}


void registerNativeSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("Native", [](ReadBuffer & buf, const FormatSettings & settings)
    {
        return std::make_shared<NativeSchemaReader>(buf, settings);
    });
}


}
