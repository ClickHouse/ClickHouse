#include <Processors/Sources/JemallocProfileSource.h>

#if USE_JEMALLOC

#    include <cmath>
#    include <limits>
#    include <list>
#    include <mutex>
#    include <ranges>
#    include <unordered_set>
#    include <Columns/ColumnString.h>
#    include <Core/Defines.h>
#    include <DataTypes/DataTypeString.h>
#    include <IO/ReadBufferFromFile.h>
#    include <IO/ReadHelpers.h>
#    include <IO/WriteBufferFromFile.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>
#    include <Processors/Executors/PullingPipelineExecutor.h>
#    include <QueryPipeline/QueryPipeline.h>
#    include <base/hex.h>
#    include <Common/FramePointers.h>
#    include <Common/MemoryTrackerSwitcher.h>
#    include <Common/StackTrace.h>
#    include <Common/StringUtils.h>
#    include <Common/getExecutablePath.h>
#    include <base/defines.h>
#    include <Common/SipHash.h>

namespace DB
{

namespace
{

/// Parse a hex address (optionally prefixed with 0x/0X) from the beginning of src,
/// advance src past the consumed characters, and return the parsed value.
/// Returns std::nullopt if no hex digit was found.
std::optional<UInt64> parseHexAddress(std::string_view & src)
{
    if (src.size() >= 2 && src[0] == '0' && (src[1] == 'x' || src[1] == 'X'))
        src.remove_prefix(2);

    if (src.empty())
        return std::nullopt;

    UInt64 address = 0;
    size_t processed = 0;

    for (size_t i = 0; i < src.size() && processed < 16; ++i)
    {
        char c = src[i];
        if (isHexDigit(c))
        {
            address = (address << 4) | unhex(c);
            ++processed;
        }
        else
            break;
    }

    if (processed == 0)
        return std::nullopt;

    src.remove_prefix(processed);
    return address;
}

/// Parse stack addresses from a jemalloc profile line starting with '@'.
/// Returns empty vector if the line doesn't start with '@'.
/// The first address is kept as-is; subsequent ones are decremented by 1
/// (they are return addresses, so we subtract 1 to point inside the call instruction).
std::vector<UInt64> parseStackAddresses(std::string_view line)
{
    std::vector<UInt64> result;
    if (line.empty() || line[0] != '@')
        return result;

    std::string_view sv(line.data() + 1, line.size() - 1);
    bool first = true;
    while (!sv.empty())
    {
        trimLeft(sv);
        if (sv.empty())
            break;
        auto address = parseHexAddress(sv);
        if (!address.has_value())
            break;
        result.push_back(first ? *address : *address - 1);
        first = false;
    }
    return result;
}

/// Parse the sampling interval from a jemalloc heap_v2 header line ("heap_v2/N").
/// Returns 0 if the header doesn't match heap_v2 format or the value is not a valid integer.
UInt64 parseSamplingInterval(std::string_view header)
{
    static constexpr std::string_view prefix = "heap_v2/";
    if (!header.starts_with(prefix))
        return 0;
    header.remove_prefix(prefix.size());
    trim(header);
    if (header.empty())
        return 0;
    UInt64 result = 0;
    ReadBufferFromMemory buf(header.data(), header.size());
    if (!tryReadIntText(result, buf) || !buf.eof())
        return 0;
    return result;
}

/// Apply Poisson sampling correction as jeprof does for heap_v2 profiles.
/// Each allocation is sampled with probability 1-exp(-size/interval), so the correction
/// factor is 1/(1-exp(-mean_size/interval)).
/// Uses std::expm1 to avoid catastrophic cancellation for small ratios where
/// 1-exp(-x) loses precision, and guards against non-finite results to prevent
/// undefined behavior in std::llround.
/// Returns the scaled metric value.
UInt64 applySamplingCorrection(UInt64 count, UInt64 bytes, UInt64 sampling_interval, bool use_count)
{
    if (sampling_interval == 0 || count == 0)
        return use_count ? count : bytes;

    if (bytes == 0)
        return use_count ? count : 0;

    double mean_size = static_cast<double>(bytes) / static_cast<double>(count);
    double ratio = mean_size / static_cast<double>(sampling_interval);
    double denom = -std::expm1(-ratio); /// accurate even for tiny ratio

    if (denom <= 0.0) /// defensive: shouldn't happen after expm1, but clamp
        return use_count ? count : bytes;

    double scale = 1.0 / denom;
    double metric = use_count ? static_cast<double>(count) : static_cast<double>(bytes);
    double corrected = metric * scale;

    if (!std::isfinite(corrected) || corrected < 0.0 || corrected > static_cast<double>(std::numeric_limits<Int64>::max()))
        return use_count ? count : bytes;

    return static_cast<UInt64>(std::llround(corrected));
}

/// Simple LRU cache: evicts the least-recently-used entry when the capacity is exceeded.
/// Key includes both the address and the symbolize_with_inline flag so that queries
/// with different inline settings don't silently reuse each other's results.
/// Value is a shared_ptr to avoid copying the symbol vector on every cache hit.
struct SymbolizationLRUCache
{
    using Key = std::pair<UInt64, bool>;
    using Value = std::shared_ptr<const std::vector<std::string>>;

    struct KeyHash
    {
        size_t operator()(const Key & k) const
        {
            SipHash hash;
            hash.update(k.first);
            hash.update(k.second);
            return hash.get64();
        }
    };

    using List = std::list<std::pair<Key, Value>>;

    explicit SymbolizationLRUCache(size_t max_size_) : max_size(max_size_) {}

    /// Returns the cached value on hit (moves the entry to the front), or nullopt on miss.
    std::optional<Value> get(const Key & key)
    {
        std::lock_guard lock(mutex);
        auto it = index.find(key);
        if (it == index.end())
            return std::nullopt;
        lru.splice(lru.begin(), lru, it->second);
        return it->second->second;
    }

    /// Inserts a new entry (or updates existing) and returns the stored shared_ptr.
    Value put(const Key & key, std::vector<std::string> value)
    {
        MemoryTrackerSwitcher switcher(&total_memory_tracker);
        auto shared = std::make_shared<const std::vector<std::string>>(std::move(value));
        std::lock_guard lock(mutex);
        auto it = index.find(key);
        if (it != index.end())
        {
            it->second->second = shared;
            lru.splice(lru.begin(), lru, it->second);
            return shared;
        }
        lru.emplace_front(key, shared);
        index.emplace(key, lru.begin());
        if (lru.size() > max_size)
        {
            index.erase(lru.back().first);
            lru.pop_back();
        }
        return shared;
    }

private:
    const size_t max_size;

    mutable std::mutex mutex;
    List lru TSA_GUARDED_BY(mutex);
    std::unordered_map<Key, List::iterator, KeyHash> index TSA_GUARDED_BY(mutex);
};

SymbolizationLRUCache symbolization_cache(/*max_size=*/ 100'000);

/// Resolve a single address to its symbol names, using the global cache.
/// Frames are stored in callback order (inline-first when enabled).
/// Callers reverse at output time as needed.
SymbolizationLRUCache::Value resolveAddress(UInt64 address, bool symbolize_with_inline)
{
    auto key = SymbolizationLRUCache::Key{address, symbolize_with_inline};
    if (auto cached = symbolization_cache.get(key))
        return *cached;

    FramePointers fp;
    fp[0] = reinterpret_cast<void *>(address);
    std::vector<std::string> frame_symbols;
    StackTrace::forEachFrame(
        fp, 0, 1,
        [&](const StackTrace::Frame & frame)
        {
            frame_symbols.push_back(frame.symbol.value_or("??"));
        },
        symbolize_with_inline);

    return symbolization_cache.put(key, std::move(frame_symbols));
}

}

JemallocProfileSource::JemallocProfileSource(
    const std::string & filename_,
    const SharedHeader & header_,
    size_t max_block_size_,
    JemallocProfileFormat mode_,
    bool symbolize_with_inline_,
    bool collapsed_use_count_)
    : ISource(header_)
    , filename(filename_)
    , max_block_size(max_block_size_)
    , mode(mode_)
    , symbolize_with_inline(symbolize_with_inline_)
    , collapsed_use_count(collapsed_use_count_)
{
}

Chunk JemallocProfileSource::generate()
{
    if (is_finished)
        return {};

    switch (mode)
    {
        case JemallocProfileFormat::Raw:
            return generateRaw();
        case JemallocProfileFormat::Symbolized:
            return generateSymbolized();
        case JemallocProfileFormat::Collapsed:
            return generateCollapsed();
    }
}

Chunk JemallocProfileSource::generateRaw()
{
    if (!file_input)
        file_input = std::make_unique<ReadBufferFromFile>(filename);

    /// Stream directly from file
    if (file_input->eof())
    {
        is_finished = true;
        return {};
    }

    auto column = ColumnString::create();

    for (size_t rows = 0; rows < max_block_size && !file_input->eof(); ++rows)
    {
        std::string line;
        readStringUntilNewlineInto(line, *file_input);
        file_input->tryIgnore(1);

        column->insertData(line.data(), line.size());
    }

    if (file_input->eof())
        is_finished = true;

    size_t num_rows = column->size();
    Columns columns;
    columns.push_back(std::move(column));

    return Chunk(std::move(columns), num_rows);
}

Chunk JemallocProfileSource::generateSymbolized()
{
    auto column = ColumnString::create();

    while (column->size() < max_block_size)
    {
        if (symbolized_phase == SymbolizedPhase::CollectingAddresses)
        {
            collectAddresses();
            symbolized_phase = SymbolizedPhase::OutputtingSymbolHeader;
        }

        if (symbolized_phase == SymbolizedPhase::OutputtingSymbolHeader)
        {
            /// Output symbol section header
            if (!symbol_header_line_output)
            {
                column->insertData("--- symbol", 10);
                symbol_header_line_output = true;
                if (column->size() >= max_block_size)
                    break;
            }

            if (!binary_line_output)
            {
                if (auto binary_path = getExecutablePath(); !binary_path.empty())
                {
                    std::string binary_line = "binary=" + binary_path;
                    column->insertData(binary_line.data(), binary_line.size());
                }
                binary_line_output = true;
                if (column->size() >= max_block_size)
                    break;
            }

            symbolized_phase = SymbolizedPhase::OutputtingSymbols;
        }

        if (symbolized_phase == SymbolizedPhase::OutputtingSymbols)
        {
            /// Stream symbol lines
            while (current_address_index < addresses.size() && column->size() < max_block_size)
            {
                if (isCancelled())
                {
                    is_finished = true;
                    break;
                }

                UInt64 address = addresses[current_address_index++];

                auto symbols = resolveAddress(address, symbolize_with_inline);

                std::string symbol_line;
                WriteBufferFromString out(symbol_line);
                writePointerHex(reinterpret_cast<const void *>(address), out);

                std::string_view separator(" ");
                for (const auto & symbol : std::ranges::reverse_view(*symbols))
                {
                    writeString(separator, out);
                    writeString(symbol, out);
                    separator = std::string_view("--");
                }
                out.finalize();

                column->insertData(symbol_line.data(), symbol_line.size());
            }

            if (current_address_index >= addresses.size())
            {
                symbolized_phase = SymbolizedPhase::OutputtingHeapHeader;
            }
            else
            {
                break; /// Chunk is full
            }
        }

        if (symbolized_phase == SymbolizedPhase::OutputtingHeapHeader)
        {
            if (!heap_separator_output)
            {
                column->insertData("---", 3);
                heap_separator_output = true;
                if (column->size() >= max_block_size)
                    break;
            }

            if (!heap_header_output)
            {
                column->insertData("--- heap", 8);
                heap_header_output = true;
                if (column->size() >= max_block_size)
                    break;
            }

            symbolized_phase = SymbolizedPhase::OutputtingHeap;
        }

        if (symbolized_phase == SymbolizedPhase::OutputtingHeap)
        {
            /// Re-read the profile file to stream heap lines (avoids storing all lines in memory)
            if (!file_input)
                file_input = std::make_unique<ReadBufferFromFile>(filename);

            while (!file_input->eof() && column->size() < max_block_size)
            {
                std::string line;
                readStringUntilNewlineInto(line, *file_input);
                file_input->tryIgnore(1);
                column->insertData(line.data(), line.size());
            }

            if (file_input->eof())
            {
                symbolized_phase = SymbolizedPhase::Done;
                is_finished = true;
            }

            break; /// Chunk is full or done
        }

        if (symbolized_phase == SymbolizedPhase::Done)
        {
            is_finished = true;
            break;
        }
    }

    if (column->empty())
    {
        is_finished = true;
        return {};
    }

    size_t num_rows = column->size();
    Columns columns;
    columns.push_back(std::move(column));

    return Chunk(std::move(columns), num_rows);
}

void JemallocProfileSource::collectAddresses()
{
    ReadBufferFromFile in(filename);

    std::unordered_set<UInt64> unique_addresses;
    std::string line;

    while (!in.eof())
    {
        if (isCancelled())
        {
            is_finished = true;
            return;
        }

        line.clear();
        readStringUntilNewlineInto(line, in);
        in.tryIgnore(1);

        for (UInt64 addr : parseStackAddresses(line))
            unique_addresses.insert(addr);
    }

    /// Convert set to sorted vector for deterministic output
    addresses.assign(unique_addresses.begin(), unique_addresses.end());
    std::sort(addresses.begin(), addresses.end());
}

Chunk JemallocProfileSource::generateCollapsed()
{
    /// Aggregate all stacks on the first call
    if (!collapsed_state)
    {
        collapsed_state.emplace();
        auto & state = *collapsed_state;

        ReadBufferFromFile in(filename);
        std::string line;
        std::vector<UInt64> current_stack;
        UInt64 sampling_interval = 0;

        while (!in.eof())
        {
            if (isCancelled())
            {
                is_finished = true;
                return {};
            }

            line.clear();
            readStringUntilNewlineInto(line, in);
            in.tryIgnore(1);

            if (line.empty())
                continue;

            /// Parse sampling interval from heap_v2/N header (first non-empty line)
            if (sampling_interval == 0 && line.starts_with("heap_v2/"))
            {
                sampling_interval = parseSamplingInterval(line);
                continue;
            }

            if (line[0] == '@')
            {
                current_stack = parseStackAddresses(line);
            }
            else if (!current_stack.empty() && line.contains(':'))
            {
                /// Each allocation record follows its `@` stack line in the jemalloc heap profile format:
                ///
                ///   @ 0x00007f1234 0x00007f5678 0x00007f9abc
                ///   t*: 1: 224 [0: 0]
                ///   t725: 1: 224 [0: 0]
                ///
                /// The record has the form `<thread>: <live_count>: <live_bytes> [<alloc_count>: <alloc_bytes>]`,
                /// where the bracketed pair holds the cumulative (total) counts since profiling started.
                /// We extract both <live_count> and <live_bytes> to apply Poisson sampling correction,
                /// then report the requested metric (bytes or count).

                size_t first_colon = line.find(':');
                size_t second_colon = line.find(':', first_colon + 1);

                if (second_colon != std::string::npos)
                {
                    /// <live_count>: between 1st and 2nd colon
                    std::string_view count_str(line.data() + first_colon + 1, second_colon - first_colon - 1);
                    trim(count_str);
                    UInt64 live_count = parseInt<UInt64>(count_str);

                    /// <live_bytes>: between 2nd colon and opening bracket (or end of line)
                    size_t bracket_pos = line.find('[', second_colon);
                    size_t end_pos = (bracket_pos != std::string::npos) ? bracket_pos : line.size();
                    std::string_view bytes_str(line.data() + second_colon + 1, end_pos - second_colon - 1);
                    trim(bytes_str);
                    UInt64 live_bytes = parseInt<UInt64>(bytes_str);

                    /// Apply Poisson sampling correction (same algorithm as jeprof's AdjustSamples).
                    UInt64 metric = applySamplingCorrection(live_count, live_bytes, sampling_interval, collapsed_use_count);

                    if (metric > 0)
                    {
                        /// Build collapsed stack string: reverse stack to get root->leaf order,
                        /// and for each address reverse the resolved frames (stored inline-first)
                        /// so that the main frame comes first within each address.
                        std::string stack_str;
                        WriteBufferFromString out(stack_str);
                        bool first_symbol = true;
                        for (UInt64 address : std::ranges::reverse_view(current_stack))
                        {
                            if (isCancelled())
                            {
                                is_finished = true;
                                return {};
                            }

                            auto symbols = resolveAddress(address, symbolize_with_inline);
                            for (const auto & symbol : std::ranges::reverse_view(*symbols))
                            {
                                if (!first_symbol)
                                    writeChar(';', out);
                                first_symbol = false;
                                writeString(symbol, out);
                            }
                        }
                        out.finalize();

                        /// Aggregate metric for same stack
                        state.stack_to_metric[stack_str] += metric;
                    }
                }

                current_stack.clear();
            }
        }

        state.iter = state.stack_to_metric.begin();
    }

    auto & state = *collapsed_state;

    /// Stream directly from the aggregated map
    if (state.iter == state.stack_to_metric.end())
    {
        is_finished = true;
        collapsed_state.reset();
        return {};
    }

    auto column = ColumnString::create();

    for (size_t rows = 0; rows < max_block_size && state.iter != state.stack_to_metric.end(); ++rows, ++state.iter)
    {
        auto formatted = fmt::format("{} {}", state.iter->first, state.iter->second);
        column->insertData(formatted.data(), formatted.size());
    }

    if (state.iter == state.stack_to_metric.end())
    {
        is_finished = true;
        collapsed_state.reset();
    }

    size_t num_rows = column->size();
    Columns columns;
    columns.push_back(std::move(column));

    return Chunk(std::move(columns), num_rows);
}

namespace
{

void pullProfileLines(
    const std::string & input_filename,
    JemallocProfileFormat format,
    bool symbolize_with_inline,
    WriteBuffer & out)
{
    Block header;
    header.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "line"});
    auto source = std::make_shared<JemallocProfileSource>(
        input_filename,
        std::make_shared<const Block>(std::move(header)),
        DEFAULT_BLOCK_SIZE,
        format,
        symbolize_with_inline);

    QueryPipeline pipeline(std::move(source));
    PullingPipelineExecutor executor(pipeline);
    Block block;
    while (executor.pull(block))
    {
        const auto & column = assert_cast<const ColumnString &>(*block.getByPosition(0).column);
        for (size_t i = 0; i < column.size(); ++i)
        {
            auto sv = column.getDataAt(i);
            out.write(sv.data(), sv.size());
            writeChar('\n', out);
        }
    }
}

}

void symbolizeJemallocHeapProfile(
    const std::string & input_filename,
    const std::string & output_filename,
    JemallocProfileFormat format,
    bool symbolize_with_inline)
{
    WriteBufferFromFile out(output_filename);
    pullProfileLines(input_filename, format, symbolize_with_inline, out);
    out.finalize();
}

std::string symbolizeJemallocHeapProfileToString(
    const std::string & input_filename,
    JemallocProfileFormat format,
    bool symbolize_with_inline)
{
    WriteBufferFromOwnString out;
    pullProfileLines(input_filename, format, symbolize_with_inline, out);
    return std::move(out.str());
}

}

#endif
