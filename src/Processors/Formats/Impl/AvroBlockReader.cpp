#include "AvroBlockReader.h"

#if USE_AVRO

#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

AvroHeaderState AvroBlockReader::extractHeaderState(avro::DataFileReaderBase & reader)
{
    AvroHeaderState state;
    state.schema = reader.dataSchema();
    state.codec = reader.codec();
    state.sync_marker = reader.sync();
    return state;
}

int64_t AvroBlockReader::readVarInt(ReadBuffer & in)
{
    Int64 value;
    DB::readVarInt(value, in);
    return value;
}

void AvroBlockReader::writeVarInt(int64_t value, std::string & out)
{
    char buf[10];  /// Max 10 bytes for int64 varint
    char * end = DB::writeVarInt(value, buf);
    out.append(buf, end - buf);
}

std::pair<int64_t, std::string> AvroBlockReader::readBlock(ReadBuffer & in)
{
    int64_t object_count = readVarInt(in);
    int64_t byte_count = readVarInt(in);

    if (byte_count < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Invalid Avro block: negative byte count {}", byte_count);

    std::string compressed_data;
    compressed_data.resize(byte_count);
    in.readStrict(compressed_data.data(), byte_count);

    return {object_count, std::move(compressed_data)};
}

bool AvroBlockReader::verifySyncMarker(ReadBuffer & in, const avro::DataFileSync & expected)
{
    if (in.eof())
        return false;

    avro::DataFileSync actual{};
    in.readStrict(reinterpret_cast<char *>(actual.data()), actual.size());

    if (actual != expected)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Avro sync marker mismatch");
    return true;
}

void AvroBlockReader::decompressBlock(const char * data, size_t size, avro::Codec codec, std::string & out)
{
    avro::DataFileReaderBase::decompressBlock(data, size, codec, out);
}

}

#endif
