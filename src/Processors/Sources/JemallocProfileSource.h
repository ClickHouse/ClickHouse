#pragma once

#include "config.h"

#if USE_JEMALLOC

#    include <memory>
#    include <optional>
#    include <string>
#    include <unordered_map>
#    include <vector>
#    include <Core/SettingsEnums.h>
#    include <IO/ReadBufferFromFile.h>
#    include <Processors/ISource.h>

namespace DB
{

/// Source that reads a jemalloc heap profile file and outputs lines according to the requested format.
///
/// Supported formats:
/// - Raw:        streams lines directly from the heap profile file.
/// - Symbolized: produces a "jeprof --raw" compatible output with embedded symbols (jeprof format).
/// - Collapsed:  produces FlameGraph-compatible collapsed stacks.
///
/// The Symbolized and Collapsed formats support a symbolize_with_inline flag: when true, inline
/// frames are resolved; when false they are skipped.
class JemallocProfileSource final : public ISource
{
public:
    JemallocProfileSource(
        const std::string & filename_,
        const SharedHeader & header_,
        size_t max_block_size_,
        JemallocProfileFormat mode_,
        bool symbolize_with_inline_,
        bool collapsed_use_count_ = false);

    String getName() const override { return "JemallocProfile"; }

protected:
    Chunk generate() override;

private:
    enum class SymbolizedPhase
    {
        CollectingAddresses,
        OutputtingSymbolHeader,
        OutputtingSymbols,
        OutputtingHeapHeader,
        OutputtingHeap,
        Done
    };

    Chunk generateRaw();
    Chunk generateSymbolized();
    Chunk generateCollapsed();
    void collectAddresses();

    std::string filename;
    std::unique_ptr<ReadBufferFromFile> file_input;
    size_t max_block_size;
    bool is_finished = false;
    JemallocProfileFormat mode;
    bool symbolize_with_inline;
    bool collapsed_use_count;

    /// For Symbolized mode streaming
    SymbolizedPhase symbolized_phase = SymbolizedPhase::CollectingAddresses;
    std::vector<UInt64> addresses;        /// Collected addresses to symbolize
    size_t current_address_index = 0;

    /// Track what we've output in header phases
    bool symbol_header_line_output = false;
    bool binary_line_output = false;
    bool heap_separator_output = false;
    bool heap_header_output = false;

    /// For Collapsed mode: aggregated stacks streamed directly from the map
    struct CollapsedState
    {
        std::unordered_map<std::string, UInt64> stack_to_metric;
        std::unordered_map<std::string, UInt64>::const_iterator iter;

        CollapsedState() = default;
        CollapsedState(const CollapsedState &) = delete;
        CollapsedState & operator=(const CollapsedState &) = delete;
        CollapsedState(CollapsedState &&) = delete;
        CollapsedState & operator=(CollapsedState &&) = delete;
    };
    std::optional<CollapsedState> collapsed_state;
};

/// Parse stack addresses from a jemalloc profile line starting with '@'.
/// Returns empty vector if the line doesn't start with '@'.
/// The first address is kept as-is; subsequent ones are decremented by 1
/// (they are return addresses, so we subtract 1 to point inside the call instruction).
/// `fully_parsed` (when given) reports whether the whole line was consumed;
/// on a malformed token the parsed prefix is still returned - strict callers
/// must check the flag, best-effort callers may ignore it.
std::vector<UInt64> parseJemallocStackAddresses(std::string_view line, bool * fully_parsed = nullptr);

/// Parse the sampling interval from a jemalloc heap_v2 header line ("heap_v2/N").
/// Returns 0 if the header doesn't match heap_v2 format or the value is not a valid integer.
UInt64 parseJemallocSamplingInterval(std::string_view header);

/// Convenience wrapper: runs JemallocProfileSource and writes every output line to output_filename.
void symbolizeJemallocHeapProfile(
    const std::string & input_filename,
    const std::string & output_filename,
    JemallocProfileFormat format = JemallocProfileFormat::Symbolized,
    bool symbolize_with_inline = true);

/// Like symbolizeJemallocHeapProfile but returns the result as a string.
std::string symbolizeJemallocHeapProfileToString(
    const std::string & input_filename,
    JemallocProfileFormat format = JemallocProfileFormat::Symbolized,
    bool symbolize_with_inline = true);

}

#endif
