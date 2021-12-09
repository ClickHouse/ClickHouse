#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>


namespace DB
{


class NativeInputFormat final : public IInputFormat
{
public:
    NativeInputFormat(ReadBuffer & buf, const Block & header)
        : IInputFormat(header, buf)
        , reader(buf, header, 0) {}

    String getName() const override { return "Native"; }

    void resetParser() override
    {
        IInputFormat::resetParser();
        reader.resetParser();
    }

    Chunk generate() override
    {
        auto block = reader.read();
        if (!block)
            return {};

        assertBlocksHaveEqualStructure(getPort().getHeader(), block, getName());
        block.checkNumberOfRows();

        size_t num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

private:
    NativeReader reader;
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

void registerInputFormatNative(FormatFactory & factory)
{
    factory.registerInputFormat("Native", [](
        ReadBuffer & buf,
        const Block & sample,
        const RowInputFormatParams &,
        const FormatSettings &)
    {
        return std::make_shared<NativeInputFormat>(buf, sample);
    });
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

}
