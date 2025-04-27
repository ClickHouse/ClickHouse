#include <IO/WriteBufferFromString.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatFactory.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <fmt/format.h>
#include <optional>

namespace DB
{

class HashOutputFormat final : public IOutputFormat
{
public:
    HashOutputFormat(const Block & header, WriteBuffer & buf, const String & /* algorithm */)
        : IOutputFormat(header, buf), hashing_buffer(buf) {}

    String getHash()
    {
        const auto h = hashing_buffer.getHash();
        std::string raw = fmt::format("{:016x}{:016x}", h.high64, h.low64);
        size_t pos = raw.find_first_not_of('0');
        if (pos == std::string::npos)
            pos = raw.size() - 1;
        return String(raw.substr(pos));
    }

    String getName() const override { return "HashOutputFormat"; }

    /// Return content type to treat output as plain text
    String getContentType() const override { return "text/plain"; }

protected:
    void consume(Chunk chunk) override
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

    void finalizeImpl() override
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

private:
    HashingWriteBuffer hashing_buffer;
};

void registerOutputFormatHash(FormatFactory & factory)
{
    factory.registerOutputFormat("Hash",
        [](WriteBuffer & buf, const Block & header, const FormatSettings &)
        {
            return std::make_shared<HashOutputFormat>(header, buf, "CRC32");
        });
}

} // namespace DB
