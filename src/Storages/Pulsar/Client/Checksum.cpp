#include <Storages/Pulsar/Client/Checksum.h>

#include <array>

namespace DB::Pulsar
{

namespace
{

/// Lookup table for the reflected CRC32C polynomial (0x82F63B78), built once at startup.
std::array<uint32_t, 256> makeTable()
{
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i)
    {
        uint32_t crc = i;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc & 1) ? (crc >> 1) ^ 0x82F63B78u : (crc >> 1);
        table[i] = crc;
    }
    return table;
}

const std::array<uint32_t, 256> crc_table = makeTable();

}

uint32_t crc32c(const char * data, size_t size)
{
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < size; ++i)
        crc = crc_table[(crc ^ static_cast<unsigned char>(data[i])) & 0xFFu] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

}
