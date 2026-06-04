#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class FormatFactory;
class ColumnBinaryOutputFormat final : public IOutputFormat
{
public:
    ColumnBinaryOutputFormat(WriteBuffer & out_, SharedHeader header,
                             bool disable_preallocation = false);

    String getName() const override { return "ColumnBinary"; }

    bool expectMaterializedColumns() const override { return false; }
    bool supportsColumnSchema() const override { return true; }
    std::optional<uint64_t> precomputeSerializedSize(const Block & block, size_t rows) const override;

protected:
    void consume(Chunk chunk) override;
    void writePrefix() override {}

    SharedHeader header_;
    bool disable_preallocation_;
};

void registerOutputFormatColumnBinary(FormatFactory & factory);

}
