#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{

class NullOutputFormat : public IOutputFormat
{
public:
    NullOutputFormat(const Block & header, WriteBuffer & out_) : IOutputFormat(header, out_) {}

    String getName() const override { return "NullOutputFormat"; }

protected:
    void consume(Chunk) override {}
};

void registerOutputFormatProcessorNull(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("Null", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams &,
        const FormatSettings &)
    {
        return std::make_shared<NullOutputFormat>(sample, buf);
    });
}

}
