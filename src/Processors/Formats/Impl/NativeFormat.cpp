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
    NativeInputFormat(ReadBuffer & buf, const Block & header_, const FormatSettings & settings)
        : IInputFormat(header_, buf)
        , reader(std::make_unique<NativeReader>(buf, header_, 0, settings.skip_unknown_fields))
        , header(header_) {}

    String getName() const override { return "Native"; }

    void resetParser() override
    {
        IInputFormat::resetParser();
        reader->resetParser();
    }

    Chunk generate() override
    {
        auto block = reader->read();
        if (!block)
            return {};

        assertBlocksHaveEqualStructure(getPort().getHeader(), block, getName());
        block.checkNumberOfRows();

        size_t num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    void setReadBuffer(ReadBuffer & in_) override
    {
        reader = std::make_unique<NativeReader>(in_, header, 0);
        IInputFormat::setReadBuffer(in_);
    }

private:
    std::unique_ptr<NativeReader> reader;
    Block header;
};

class NativeOutputFormat final : public IOutputFormat
{
public:
    NativeOutputFormat(WriteBuffer & buf, const Block & header)
        : IOutputFormat(header, buf)
        , writer(buf, 0, header)
    {
    }

    String getName() const override { return "Native"; }

    std::string getContentType() const override
    {
        return writer.getContentType();
    }

protected:
    void consume(Chunk chunk) override
    {
        if (chunk)
        {
            auto block = getPort(PortKind::Main).getHeader();

            // const auto & info = chunk.getChunkInfo();
            // const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
            // if (agg_info)
            // {
            //     block.info.bucket_num = agg_info->bucket_num;
            //     block.info.is_overflows = agg_info->is_overflows;
            // }

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
    explicit NativeSchemaReader(ReadBuffer & in_) : ISchemaReader(in_) {}

    NamesAndTypesList readSchema() override
    {
        auto reader = NativeReader(in, 0);
        auto block = reader.read();
        return block.getNamesAndTypesList();
    }
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
        const RowOutputFormatParams &,
        const FormatSettings &)
    {
        return std::make_shared<NativeOutputFormat>(buf, sample);
    });
}


void registerNativeSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("Native", [](ReadBuffer & buf, const FormatSettings &)
    {
        return std::make_shared<NativeSchemaReader>(buf);
    });
}


}
