#include <Storages/MergeTree/MergeTreeDataPartDeletedMask.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/CompressionMethod.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/NativeWriter.h>
#include <Formats/NativeReader.h>

#include <Common/SipHash.h>


namespace DB::ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int CORRUPTED_DATA;
}

namespace DB
{

namespace
{
struct DeletedRowsHash
{
private:
    char value[16];

public:
    DeletedRowsHash() = default;
    explicit DeletedRowsHash(SipHash & hasher)
    {
        hasher.get128(value);

        static_assert(std::is_pod_v<DeletedRowsHash>, "Expected to be a POD-type");
        static_assert(sizeof(DeletedRowsHash) * 8 == 128);
    }

    bool operator==(const DeletedRowsHash & other) const
    {
        return memcmp(value, other.value, sizeof(value)) == 0;
    }
};

constexpr UInt8 FORMAT_VERSION = 1;
constexpr UInt8 DEFAULT_CODEC = static_cast<UInt8>(CompressionMethodByte::T64);
constexpr UInt8 PADDING_SIZE = 7; // just in case
constexpr UInt8 HEADER_SIZE = 0
        + sizeof(FORMAT_VERSION)
        + sizeof(UInt64)                  // number of rows in mask
        + sizeof(DeletedRowsHash)         // column data hash
        + PADDING_SIZE                    // padding: zero-bytes
        + sizeof(DeletedRowsHash);        // header hash
}

MergeTreeDataPartDeletedMask::MergeTreeDataPartDeletedMask()
    : deleted_rows(ColumnUInt8::create())
{}

const ColumnUInt8 & MergeTreeDataPartDeletedMask::getDeletedRows() const
{
    return *deleted_rows;
}

void MergeTreeDataPartDeletedMask::setDeletedRows(DeletedRows new_rows)
{
    deleted_rows.swap(new_rows);
}

void MergeTreeDataPartDeletedMask::setDeletedRows(size_t rows, bool value)
{
    setDeletedRows(ColumnUInt8::create(rows, value));
}

void MergeTreeDataPartDeletedMask::read(ReadBuffer & in)
{
    std::array<char, HEADER_SIZE - sizeof(DeletedRowsHash)> header_buffer_data;
    in.readStrict(header_buffer_data.data(), header_buffer_data.size());
    {// validate hash of the header first
        SipHash hash;
        hash.update(header_buffer_data.data(), header_buffer_data.size());
        const DeletedRowsHash computed_hash(hash);

        DeletedRowsHash read_hash;
        readPODBinary(read_hash, in);
        if (read_hash != computed_hash)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Invalid deleted masks file header hash");
    }

    UInt8 format_version = FORMAT_VERSION;
    UInt64 stored_rows = 0;
    DeletedRowsHash column_hash;
    {// Read header values
        ReadBuffer header(header_buffer_data.data(), header_buffer_data.size(), 0);
        readBinary(format_version, header);
        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
                    "Unknown deleted mask file format version {}",
                    static_cast<UInt32>(format_version));

        readBinary(stored_rows, header);
        readPODBinary(column_hash, header);
        header.ignore(PADDING_SIZE);
        assertEOF(header);
    }

    auto data_read_buffer = std::make_unique<CompressedReadBuffer>(in);

    auto res_column = DeletedRows(ColumnUInt8::create());
    ColumnPtr res_col_ptr = res_column;
    SerializationPtr serialization = DataTypeUInt8().getDefaultSerialization();
    NativeReader::readData(*serialization, res_col_ptr, *data_read_buffer, stored_rows, 0);
    assertEOF(*data_read_buffer);

    // we probably don't want to check column hash here, since codec verifies data integrity.
    deleted_rows = res_column;
}

void MergeTreeDataPartDeletedMask::write(WriteBuffer & out) const
{
    {// Header
        std::array<char, HEADER_SIZE - sizeof(DeletedRowsHash)> header_buffer_data;
        WriteBuffer header(header_buffer_data.data(), header_buffer_data.size());

        writeBinary(FORMAT_VERSION, header);
        writeBinary(static_cast<UInt64>(deleted_rows->size()), header);

        {
            SipHash hash;
            deleted_rows->updateHashFast(hash);
            writePODBinary(DeletedRowsHash(hash), header);
        }

        {
            const char padding[PADDING_SIZE] = {'\0'};
            writePODBinary(padding, header);
        }
        assert(header_buffer_data.max_size() == header.count());

        writePODBinary(header_buffer_data, out);
        {// header hash
            SipHash hash;
            hash.update(header_buffer_data.data(), header_buffer_data.size());
            writePODBinary(DeletedRowsHash(hash), out);
        }
    }
    assert(HEADER_SIZE == out.count());

    const DataTypeUInt8 col_datatype;
    auto codec = CompressionCodecFactory::instance().get(static_cast<UInt8>(DEFAULT_CODEC), &col_datatype);
    auto data_write_buffer = std::make_unique<CompressedWriteBuffer>(out, codec);
    SerializationPtr serialization = col_datatype.getDefaultSerialization();

    NativeWriter::writeData(*serialization, deleted_rows, *data_write_buffer, 0, deleted_rows->size());
    data_write_buffer->finalize();
}

}
