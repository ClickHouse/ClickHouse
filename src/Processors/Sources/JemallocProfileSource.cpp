#include <Processors/Sources/JemallocProfileSource.h>

#if USE_JEMALLOC

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

/// Simple LRU cache: evicts the least-recently-used entry when the capacity is exceeded.
struct SymbolizationLRUCache
{
    using Value = std::vector<std::string>;
    using List = std::list<std::pair<UInt64, Value>>;

    explicit SymbolizationLRUCache(size_t max_size_) : max_size(max_size_) {}

    /// Returns a copy of the cached value on hit (moves the entry to the front), or nullopt on miss.
    std::optional<Value> get(UInt64 key)
    {
        std::lock_guard lock(mutex);
        auto it = index.find(key);
        if (it == index.end())
            return std::nullopt;
        lru.splice(lru.begin(), lru, it->second);
        return it->second->second;
    }

    void put(UInt64 key, Value value)
    {
        MemoryTrackerSwitcher switcher(&total_memory_tracker);
        std::lock_guard lock(mutex);
        auto it = index.find(key);
        if (it != index.end())
        {
            it->second->second = std::move(value);
            lru.splice(lru.begin(), lru, it->second);
            return;
        }
        lru.emplace_front(key, std::move(value));
        index.emplace(key, lru.begin());
        if (lru.size() > max_size)
        {
            index.erase(lru.back().first);
            lru.pop_back();
        }
    }


private:
    const size_t max_size;

    mutable std::mutex mutex;
    List lru TSA_GUARDED_BY(mutex);
    std::unordered_map<UInt64, List::iterator> index TSA_GUARDED_BY(mutex);
};

SymbolizationLRUCache symbolization_cache(/*max_size=*/ 100'000);

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

                std::vector<std::string> symbols;
                if (auto cached = symbolization_cache.get(address))
                    symbols = std::move(*cached);

                if (symbols.empty())
                {
                    FramePointers fp;
                    fp[0] = reinterpret_cast<void *>(address);

                    auto symbolize_callback = [&](const StackTrace::Frame & frame)
                    {
                        symbols.push_back(frame.symbol.value_or("??"));
                    };

                    bool resolve_inlines = symbolize_with_inline;
                    StackTrace::forEachFrame(fp, 0, 1, symbolize_callback, /* fatal= */ resolve_inlines);
                    symbolization_cache.put(address, symbols);
                }

                std::string symbol_line;
                WriteBufferFromString out(symbol_line);
                writePointerHex(reinterpret_cast<const void *>(address), out);

                std::string_view separator(" ");
                for (const auto & symbol : std::ranges::reverse_view(symbols))
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
            /// Stream heap lines from stored profile
            while (current_profile_line_index < profile_lines.size() && column->size() < max_block_size)
            {
                const auto & line = profile_lines[current_profile_line_index++];
                column->insertData(line.data(), line.size());
            }

            if (current_profile_line_index >= profile_lines.size())
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

        profile_lines.push_back(line);

        if (line.empty())
            continue;

        /// Stack traces start with '@' followed by hex addresses
        if (line[0] == '@')
        {
            std::string_view line_addresses(line.data() + 1, line.size() - 1);

            bool first = true;
            while (!line_addresses.empty())
            {
                trimLeft(line_addresses);
                if (line_addresses.empty())
                    break;

                auto address = parseHexAddress(line_addresses);
                if (!address.has_value())
                    break;

                unique_addresses.insert(first ? address.value() : address.value() - 1);
                first = false;
            }
        }
    }

    /// Convert set to vector for iteration
    addresses.assign(unique_addresses.begin(), unique_addresses.end());
}

Chunk JemallocProfileSource::generateCollapsed()
{
    /// For collapsed mode, we need to aggregate first, so we still use vector approach
    if (collapsed_lines.empty())
    {
        ReadBufferFromFile in(filename);

        std::unordered_map<std::string, UInt64> stack_to_metric;
        std::string line;
        std::vector<UInt64> current_stack;

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

            if (line[0] == '@')
            {
                current_stack.clear();
                std::string_view line_addresses(line.data() + 1, line.size() - 1);

                bool first = true;
                while (!line_addresses.empty())
                {
                    trimLeft(line_addresses);
                    if (line_addresses.empty())
                        break;

                    auto address = parseHexAddress(line_addresses);
                    if (!address.has_value())
                        break;

                    current_stack.push_back(first ? address.value() : address.value() - 1);
                    first = false;
                }
            }
            else if (!current_stack.empty() && line.find(':') != std::string::npos)
            {
                /// Each allocation record follows its `@` stack line in the jemalloc heap profile format:
                ///
                ///   @ 0x00007f1234 0x00007f5678 0x00007f9abc
                ///   t*: 1: 224 [0: 0]
                ///   t725: 1: 224 [0: 0]
                ///
                /// The record has the form `<thread>: <live_count>: <live_bytes> [<alloc_count>: <alloc_bytes>]`,
                /// where the bracketed pair holds the cumulative (total) counts since profiling started.
                /// We extract either <live_bytes> (between the 2nd and 3rd colon) or <live_count>
                /// (between the 1st and 2nd colon) depending on the `collapsed_use_count` setting.

                size_t first_colon = line.find(':');
                size_t second_colon = line.find(':', first_colon + 1);

                if (second_colon != std::string::npos)
                {
                    std::string_view metric_str;
                    if (collapsed_use_count)
                    {
                        /// <live_count>: between 1st and 2nd colon
                        metric_str = std::string_view(line.data() + first_colon + 1, second_colon - first_colon - 1);
                    }
                    else
                    {
                        /// <live_bytes>: between 2nd colon and opening bracket (or end of line)
                        size_t bracket_pos = line.find('[', second_colon);
                        size_t end_pos = (bracket_pos != std::string::npos) ? bracket_pos : line.size();
                        metric_str = std::string_view(line.data() + second_colon + 1, end_pos - second_colon - 1);
                    }
                    trim(metric_str);

                    UInt64 metric = parseInt<UInt64>(metric_str);

                    if (metric > 0)
                    {
                        /// Symbolize stack
                        std::vector<std::string> all_symbols;

                        /// Reverse stack to get root->leaf order
                        for (UInt64 address : std::ranges::reverse_view(current_stack))
                        {
                            if (isCancelled())
                            {
                                is_finished = true;
                                return {};
                            }

                            std::vector<std::string> addr_symbols;
                            if (auto cached = symbolization_cache.get(address))
                                addr_symbols = std::move(*cached);
                            if (!addr_symbols.empty())
                            {
                                for (const auto & symbol : addr_symbols)
                                    all_symbols.push_back(symbol);
                            }
                            else
                            {
                                FramePointers fp;
                                fp[0] = reinterpret_cast<void *>(address);

                                std::vector<std::string> frame_symbols;
                                auto symbolize_callback = [&](const StackTrace::Frame & frame)
                                {
                                    frame_symbols.push_back(frame.symbol.value_or("??"));
                                };

                                bool resolve_inlines = symbolize_with_inline;
                                StackTrace::forEachFrame(fp, 0, 1, symbolize_callback, /* fatal= */ resolve_inlines);

                                /// Store in cache (in reverse order for easier reuse)
                                for (const auto & symbol : std::ranges::reverse_view(frame_symbols))
                                {
                                    addr_symbols.push_back(symbol);
                                    all_symbols.push_back(symbol);
                                }
                                symbolization_cache.put(address, addr_symbols);
                            }
                        }

                        /// Build collapsed stack string
                        std::string stack_str;
                        bool first_symbol = true;
                        for (const auto & symbol : all_symbols)
                        {
                            if (!first_symbol)
                                stack_str += ';';
                            first_symbol = false;
                            stack_str += symbol;
                        }

                        /// Aggregate metric for same stack
                        stack_to_metric[stack_str] += metric;
                    }
                }

                current_stack.clear();
            }
        }

        /// Store aggregated stacks as lines
        for (const auto & [stack, metric] : stack_to_metric)
        {
            collapsed_lines.push_back(fmt::format("{} {}", stack, metric));
        }
    }

    /// Stream from collapsed lines
    if (current_collapsed_line_index >= collapsed_lines.size())
    {
        is_finished = true;
        return {};
    }

    auto column = ColumnString::create();

    for (size_t rows = 0; rows < max_block_size && current_collapsed_line_index < collapsed_lines.size(); ++rows)
    {
        const auto & line = collapsed_lines[current_collapsed_line_index++];
        column->insertData(line.data(), line.size());
    }

    size_t num_rows = column->size();
    Columns columns;
    columns.push_back(std::move(column));

    return Chunk(std::move(columns), num_rows);
}

void symbolizeJemallocHeapProfile(
    const std::string & input_filename,
    const std::string & output_filename,
    JemallocProfileFormat format,
    bool symbolize_with_inline)
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
    WriteBufferFromFile out(output_filename);
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
    out.finalize();
}

}

#endif
