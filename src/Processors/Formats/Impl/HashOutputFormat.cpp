#include<Processors/Formats/Impl/HashOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Common/HashTable/Hash.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <fmt/format.h>

namespace DB
{

HashOutputFormat::HashOutputFormat(const Block & header, WriteBuffer & buf, const String & /* algorithm */)
    : IOutputFormat(header, buf), hashing_buffer(buf)
{

}

String HashOutputFormat::getName() const
{
    return "HashOutputFormat";
}

String HashOutputFormat::getHash()
{
    const auto h = hashing_buffer.getHash();
    std::string raw = fmt::format("{:016x}{:016x}", h.high64, h.low64);
    size_t pos = raw.find_first_not_of('0');
    if (pos == std::string::npos)
        pos = raw.size() - 1;
    return String(raw.substr(pos));
}

void HashOutputFormat::consume(Chunk chunk)
{
    Arena arena;
    const char * begin = nullptr;

    for (const auto & column : chunk.getColumns())
    {
        for (size_t i = 0; i < column->size(); ++i)
        {
            StringRef ref = column->serializeValueIntoArena(i, arena, begin);
            hashing_buffer.write(ref.data, ref.size);
        }
    }
}

void HashOutputFormat::finalizeImpl()
{
    hashing_buffer.finalize();
    const auto h = hashing_buffer.getHash();
    std::string raw = fmt::format("{:016x}{:016x}", h.high64, h.low64);
    size_t pos = raw.find_first_not_of('0');
    if (pos == std::string::npos)
        pos = raw.size() - 1;
    String stripped = raw.substr(pos) + "\n";
    out.write(stripped.data(), stripped.size());
    out.next();
}

void registerOutputFormatHash(FormatFactory & factory)
{
    factory.registerOutputFormat("Hash",
        [](WriteBuffer & buf, const Block & header, const FormatSettings &)
        {
            return std::make_shared<HashOutputFormat>(header, buf, "CRC32");
        });
}

} // namespace DB
