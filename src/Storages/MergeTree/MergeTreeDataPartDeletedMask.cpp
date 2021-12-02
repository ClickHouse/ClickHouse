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

#include <base/iostream_debug_helpers.h>


namespace DB::ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int HASH_MISMATCH_FOR_DELETED_MASK;
    extern const int CORRUPTED_DATA;
}

namespace DB
{

struct HashValue
{
private:
    char value[16];

public:
    HashValue() = default;
    HashValue(SipHash & hasher)
    {
        hasher.get128(value);

        static_assert(std::is_pod_v<HashValue>, "Expected to be a POD-type");
        static_assert(sizeof(HashValue) * 8 == 128);
    }

    bool operator==(const HashValue & other) const
    {
        return memcmp(value, other.value, sizeof(value)) == 0;
    }
};

//CompressionCodecPtr getCodec(const String & name, const IDataType & data_type)
//{
//    const String codec_statement = "(" + name + ")";
//    Tokens tokens(codec_statement.begin().base(), codec_statement.end().base());
//    IParser::Pos token_iterator(tokens, 0);

//    Expected expected;
//    ASTPtr codec_ast;
//    ParserCodec parser;

//    parser.parse(token_iterator, codec_ast, expected);

//    return CompressionCodecFactory::instance().get(codec_ast, &data_type);
//}

constexpr UInt8 FORMAT_VERSION = 1;
constexpr UInt8 DEFAULT_CODEC = static_cast<UInt8>(CompressionMethodByte::T64);
constexpr UInt8 DEFAULT_COMPRESSION = static_cast<UInt8>(CompressionMethod::Lz4);
constexpr UInt8 COMPRESSION_LEVEL = 0;
constexpr UInt8 PADDING_SIZE = 6; // just in case
constexpr UInt8 HEADER_SIZE =
        sizeof(FORMAT_VERSION)
        + sizeof(UInt64)                  // number of rows in mask
        + sizeof(HashValue)               // 128-bit column hash
        + sizeof(DEFAULT_COMPRESSION)
        + PADDING_SIZE
        + sizeof(HashValue);              // header hash

MergeTreeDataPartDeletedMask::MergeTreeDataPartDeletedMask()
{}

void MergeTreeDataPartDeletedMask::read(ReadBuffer & in)
{
    std::array<char, HEADER_SIZE - sizeof(HashValue)> header_buffer_data;
    in.readBig(header_buffer_data.data(), header_buffer_data.size());
    {// validate hash of the header first
        SipHash hash;
        hash.update(header_buffer_data.data(), header_buffer_data.size());
        const HashValue computed_hash(hash);

        HashValue read_hash;
        readPODBinary(read_hash, in);
        if (read_hash != computed_hash)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Invalid deleted masks file header hash");
    }

    UInt8 format_version = FORMAT_VERSION;
    UInt64 stored_rows = 0;
    UInt8 compression = DEFAULT_COMPRESSION;
    HashValue column_hash;
    {// Read header
        ReadBuffer header(header_buffer_data.data(), header_buffer_data.size(), 0);
        readBinary(format_version, header);
        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
                    "Unknown deleted mask file format version {}", static_cast<UInt32>(format_version));

        readBinary(stored_rows, header);
        readPODBinary(column_hash, header);
        readBinary(compression, header);
        header.ignore(PADDING_SIZE);
        assertEOF(header);
    }

    auto data_read_buffer = wrapReadBufferWithCompressionMethod(
            std::make_unique<CompressedReadBuffer>(in),
            static_cast<CompressionMethod>(compression));

    auto res_column = DeletedRows(ColumnUInt8::create());
    ColumnPtr res_col_ptr = res_column;
    NativeReader::readData(DataTypeUInt8(), res_col_ptr, *data_read_buffer, stored_rows, 0);
    assertEOF(*data_read_buffer);

    // we probably don't want to check column hash here, since codec verifies data integrity.
    deleted_rows = res_column;
}

void MergeTreeDataPartDeletedMask::write(WriteBuffer & out) const
{
    {
        std::array<char, HEADER_SIZE - sizeof(HashValue)> header_buffer_data;
        WriteBuffer header(header_buffer_data.data(), header_buffer_data.size());

        writeBinary(FORMAT_VERSION, header);
        writeBinary(static_cast<UInt64>(deleted_rows->size()), header);

        {
            SipHash hash;
            deleted_rows->updateHashFast(hash);
            writePODBinary(HashValue(hash), header);
        }
        writeBinary(DEFAULT_COMPRESSION, header);
        {
            const char padding[PADDING_SIZE] = {'\0'};
            writePODBinary(padding, header);
        }
        assert(header_buffer_data.max_size() == header.count());

        writePODBinary(header_buffer_data, out);
        {
            SipHash hash;
            hash.update(header_buffer_data.data(), header_buffer_data.size());
            writePODBinary(HashValue(hash), out);
        }
    }
    assert(HEADER_SIZE == out.count());

    DataTypeUInt8 col_datatype;
    auto codec = CompressionCodecFactory::instance().get(DEFAULT_CODEC, &col_datatype);

    auto data_write_buffer = wrapWriteBufferWithCompressionMethod(
                std::make_unique<CompressedWriteBuffer>(out, codec),
            static_cast<CompressionMethod>(DEFAULT_COMPRESSION),
            COMPRESSION_LEVEL);

    NativeWriter::writeData(col_datatype, deleted_rows, *data_write_buffer, 0, deleted_rows->size());
}
}
