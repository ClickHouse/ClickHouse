#include <Processors/Formats/Impl/ColumnBinaryInputFormat.h>

#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Formats/ColumnarV1Wire.h>

namespace DB
{

ColumnBinaryInputFormat::ColumnBinaryInputFormat(
    ReadBuffer & buf,
    const Block & header,
    const RowInputFormatParams & /*params*/,
    const FormatSettings & /*settings*/)
    : IInputFormat(std::make_shared<const Block>(header), &buf)
    , header_(std::make_shared<const Block>(header))
{
}

Chunk ColumnBinaryInputFormat::read()
{
    if (eof_)
        return {};
    if (in->eof())
    {
        eof_ = true;
        return {};
    }

    size_t avail = in->available();
    if (avail < ColumnarV1::COLUMNAR_HEADER_BYTES + ColumnarV1::COLUMNAR_DESC_BYTES)
    {
        eof_ = true;
        return {};
    }

    std::span<const uint8_t> buf{reinterpret_cast<const uint8_t *>(in->position()), avail};

    uint32_t num_rows = 0;
    uint32_t num_cols = 0;
    std::memcpy(&num_rows, buf.data(),     4);
    std::memcpy(&num_cols, buf.data() + 4, 4);

    uint32_t cols_to_read = std::min(num_cols, static_cast<uint32_t>(header_->columns()));
    MutableColumns result;
    result.reserve(cols_to_read);
    for (uint32_t i = 0; i < cols_to_read; ++i)
    {
        ColumnarV1::ColDescriptor desc{};
        std::memcpy(&desc,
                    buf.data() + ColumnarV1::COLUMNAR_HEADER_BYTES + i * ColumnarV1::COLUMNAR_DESC_BYTES,
                    sizeof(desc));
        result.push_back(ColumnarV1::readColumnFromDesc(buf, desc, num_rows,
                                                        header_->getByPosition(i).type));
    }

    in->position() += avail;
    eof_ = true;
    return Chunk(std::move(result), num_rows);
}

void registerInputFormatColumnBinary(FormatFactory & factory)
{
    factory.registerInputFormat("ColumnBinary", [](
        ReadBuffer & buf,
        const Block & header,
        const RowInputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<ColumnBinaryInputFormat>(buf, header, params, settings);
    });
}

}
