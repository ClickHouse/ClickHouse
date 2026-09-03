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
                             bool disable_preallocation = false,
                             UInt64 max_frame_size = 1024ull * 1024 * 1024);

    String getName() const override { return "ColumnBinary"; }

    bool expectMaterializedColumns() const override { return false; }
    bool supportsColumnSchema() const override { return true; }
    std::optional<uint64_t> precomputeSerializedSize(const Block & block, size_t rows) const override;

    /// Enforce the exact column count the reader requires; see consume().
    void checkNumCols(size_t num_cols) const;

    /// Enforce the exact column type the reader requires; see checkColumnStructure().
    void checkColumnStructure(size_t i, const IColumn & column) const;

protected:
    void consume(Chunk chunk) override;
    void writePrefix() override {}

    SharedHeader header_;
    bool disable_preallocation_;
    UInt64 max_frame_size_;
};

void registerOutputFormatColumnBinary(FormatFactory & factory);

}
