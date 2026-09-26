#pragma once

#include <deque>
#include <Core/Block.h>
#include <base/types.h>

namespace DB
{

/// Buffers the queries recently executed in the interactive client session, together with a
/// token-frugal sample of their results (the first and the last formatted rows) and error
/// messages. The buffer serves two purposes for the embedded AI agent:
/// - it provides the agent with the context of what the user has been doing;
/// - it captures the outcome of the queries the agent itself runs through the client,
///   so the tool results can be reported back to the model.
class QueryContextBuffer
{
public:
    struct Entry
    {
        UInt64 seqno = 0;
        String query;
        String header;                  /// `name:Type` pairs of the result columns, tab-separated
        std::vector<String> head_lines; /// first rows of the result, formatted as TSV
        std::deque<String> tail_lines;  /// last rows of the result, formatted as TSV
        size_t result_rows = 0;
        double elapsed_seconds = 0.0;
        String error;                   /// exception message, empty on success
        bool cancelled = false;
        bool from_ai = false;           /// the query was initiated by the AI agent, not typed by the user
        bool finished = false;
    };

    static constexpr size_t max_entries = 12;
    static constexpr size_t head_rows = 10;
    static constexpr size_t tail_rows = 10;
    static constexpr size_t max_line_bytes = 500;
    static constexpr size_t max_query_bytes = 2000;
    static constexpr size_t max_error_bytes = 2000;

    /// Open a new entry. An entry left unfinished (e.g. after a lost connection) is closed as is.
    void startQuery(const String & query, bool from_ai);

    /// Sample a result block of the current query. Blocks with zero rows contribute the header only.
    void addBlock(const Block & block);

    /// Attach an error message to the open entry. When there is no open entry (e.g. the query
    /// failed to parse and was never started), a standalone finished entry is created from
    /// `fallback_query`, marked with `from_ai` (whether the failed query was initiated by the
    /// AI agent rather than typed by the user; ignored when an open entry exists - it already
    /// carries the flag). The first recorded error wins; later calls for the same entry are ignored.
    void recordError(const String & fallback_query, const String & message, bool from_ai);

    /// Close the open entry.
    void finishQuery(double elapsed_seconds, bool cancelled);

    /// The sequence number of the latest entry, 0 when the buffer is empty.
    UInt64 latestSeqno() const;

    bool empty() const { return entries.empty(); }

    /// Render the entries with seqno > since_seqno as text for the model.
    /// Entries initiated by the AI agent are skipped when `skip_ai_initiated` is set
    /// (the agent has already seen them as its own tool results).
    String format(UInt64 since_seqno, bool skip_ai_initiated) const;

private:
    std::deque<Entry> entries;
    UInt64 next_seqno = 1;

    Entry * openEntry();
    static void formatEntry(const Entry & entry, WriteBuffer & out);
};

/// Format a whole block as tab-separated text with a `name:Type` header line, for feeding
/// query results to the model. The output is capped in rows and bytes; a truncation notice
/// is appended when the cap is hit.
String formatBlockAsTextForAI(const Block & block);

}
