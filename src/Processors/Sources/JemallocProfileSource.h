#pragma once

#include "config.h"

#if USE_JEMALLOC

#    include <memory>
#    include <string>
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
class JemallocProfileSource : public ISource
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
    std::vector<std::string> profile_lines;  /// Raw profile lines for heap section
    size_t current_profile_line_index = 0;

    /// Track what we've output in header phases
    bool symbol_header_line_output = false;
    bool binary_line_output = false;
    bool heap_separator_output = false;
    bool heap_header_output = false;

    /// For Collapsed mode: processed lines (need aggregation, can't stream)
    std::vector<std::string> collapsed_lines;
    size_t current_collapsed_line_index = 0;


};

/// Convenience wrapper: runs JemallocProfileSource and writes every output line to output_filename.
void symbolizeJemallocHeapProfile(
    const std::string & input_filename,
    const std::string & output_filename,
    JemallocProfileFormat format = JemallocProfileFormat::Symbolized,
    bool symbolize_with_inline = true);

}

#endif
