#pragma once

/// In-memory Transport for deterministic connection tests: captures written
/// bytes, serves scripted response chunks (with configurable fragmentation),
/// injects EOF. No sockets, no threads — the synchronous design makes every
/// error path reachable without a simulator.

#include <Storages/Kafka/Clients/Net/Transport.h>

#include <base/types.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <variant>
#include <vector>

namespace Kafka::Tests
{

class ScriptedTransport final : public Net::Transport
{
public:
    /// Queue response bytes, delivered in chunks of at most `chunk_size`.
    void script(String bytes, size_t chunk_size = SIZE_MAX)
    {
        size_t pos = 0;
        while (pos < bytes.size())
        {
            const size_t n = std::min(chunk_size, bytes.size() - pos);
            actions.emplace_back(Chunk{bytes.substr(pos, n)});
            pos += n;
        }
    }

    void scriptEof() { actions.emplace_back(Eof{}); }

    const String & getWritten() const { return written; }

    /// Written bytes split into frames (i32 size prefix stripped).
    std::vector<String> writtenFrames() const
    {
        std::vector<String> frames;
        size_t pos = 0;
        while (pos + 4 <= written.size())
        {
            Int32 size = 0;
            for (int i = 0; i < 4; ++i)
                size = (size << 8) | static_cast<UInt8>(written[pos + i]);
            frames.push_back(written.substr(pos + 4, static_cast<size_t>(size)));
            pos += 4 + static_cast<size_t>(size);
        }
        return frames;
    }

    void connect(const String & host, UInt16 port, const Net::Deadline &) override
    {
        connected_host = host;
        connected_port = port;
        open = true;
    }

    String connected_host;
    UInt16 connected_port = 0;

    size_t readSome(char * data, size_t size, const Net::Deadline &) override
    {
        if (actions.empty())
            throw Exception(ErrorKind::Timeout, "Scripted transport has no more data");
        if (std::holds_alternative<Eof>(actions.front()))
            throw Exception(ErrorKind::Connection, "Connection closed by broker");
        auto & chunk = std::get<Chunk>(actions.front());
        const size_t n = std::min(size, chunk.data.size());
        std::copy_n(chunk.data.data(), n, data);
        chunk.data.erase(0, n);
        if (chunk.data.empty())
            actions.pop_front();
        return n;
    }

    void writeAll(std::string_view data, const Net::Deadline &) override { written.append(data); }

    bool readable() override { return readable_enabled && !actions.empty(); }
    /// Hide scripted data from the readable() probe (blocking reads still see
    /// it) — lets tests force pure pipelining without opportunistic drains.
    void setReadableProbe(bool enabled) { readable_enabled = enabled; }

    void close() override { open = false; }
    bool isOpen() const override { return open; }

private:
    struct Chunk
    {
        String data;
    };
    struct Eof
    {
    };

    std::deque<std::variant<Chunk, Eof>> actions;
    String written;
    bool open = false;
    bool readable_enabled = true;
};

}
