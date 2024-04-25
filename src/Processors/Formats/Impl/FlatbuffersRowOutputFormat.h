
#include <Processors/Formats/IRowOutputFormat.h>

#ifdef USE_FLATBUFFERS

#include "flatbuffers/flexbuffers.h"

namespace DB 
{

namespace fxb = flexbuffers;

class FlatbuffersRowOutputFormat final : public IRowOutputFormat 
{
public:
    FlatbuffersRowOutputFormat(const Block & header_, WriteBuffer & out_, const FormatSettings & format_settings_);

    String getName() const override { return "FlatbuffersRowOutputFormat"; }

private:
    void writePrefix() override;
    void writeSuffix() override;
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    void serializeField(const IColumn & column, DataTypePtr data_type, size_t row_num);

    fxb::Builder builder;
};

}

#endif