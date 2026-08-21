#pragma once

#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowInputFormat.h>

#include <span>

namespace DB
{

class ColumnBinaryInputFormat final : public IInputFormat
{
public:
    ColumnBinaryInputFormat(ReadBuffer & buf, const Block & header,
                            const RowInputFormatParams & params,
                            const FormatSettings & settings);

    String getName() const override { return "ColumnBinary"; }
    Chunk read() override;

private:
    /// Validates the descriptor table in `hdr_desc` (which must cover the frame header and all
    /// `num_cols` descriptors) and returns the frame's total size, i.e. the furthest byte any
    /// descriptor references. Shared by both the in-place and the copying branch of `read`, so
    /// that a frame is validated identically no matter how it was obtained.
    /// `num_cols` comes straight from the (network-facing) frame and is otherwise untrusted;
    /// this rejects it before anything is sized off of it.
    void checkNumCols(uint32_t num_cols) const;

    uint64_t validateDescriptorsAndGetFrameEnd(std::span<const uint8_t> hdr_desc, uint32_t num_cols, size_t hdr_desc_size) const;

    SharedHeader header_;
    FormatSettings format_settings_;
    bool eof_ = false;
};

void registerInputFormatColumnBinary(FormatFactory & factory);

}
