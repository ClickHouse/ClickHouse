#include <Client/AI/QueryContextBuffer.h>

#include <Client/AI/AITextTruncation.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

#include <exception>

namespace DB
{

namespace
{

String truncateForContext(const String & text, size_t max_bytes)
{
    /// The result goes into the prompt of the model, which is serialized as JSON, so it must be valid
    /// UTF-8: a value of a `String` column can hold arbitrary bytes, and so can a query or an error.
    String truncated = escapeInvalidUTF8(text);
    if (truncated.size() <= max_bytes)
        return truncated;
    /// For the same reason the cut must not split a UTF-8 sequence.
    truncateToUTF8Boundary(truncated, max_bytes);
    return truncated + "…";
}

/// Keeps the first bytes of a line and stops the serialization once it has them, so that the
/// sample of a huge value (a long `String`, an `Array` of millions of elements) costs no more than
/// the sample itself: the line is never serialized beyond what is kept, nor copied as a whole.
class LineSampleWriteBuffer final : public WriteBufferFromPointer
{
public:
    /// Thrown out of the serialization when the buffer is full.
    struct Full : std::exception
    {
    };

    LineSampleWriteBuffer() : WriteBufferFromPointer(data, sizeof(data)) {}

    std::string_view sample() const { return full ? std::string_view(data, sizeof(data)) : std::string_view(data, offset()); }

private:
    void nextImpl() override
    {
        full = true;
        throw Full{};
    }

    /// One byte over the limit, for the truncation to tell that the line goes on.
    char data[QueryContextBuffer::max_line_bytes + 1]{};
    bool full = false;
};

/// The columns whose values are not sampled: the text of an aggregate function state is its
/// binary serialization, which says nothing to the model, and it is built whole in memory before
/// the first byte of it is written - the bounded buffer of the sample cannot stop that, so the
/// state of `groupArrayState` over millions of rows would be serialized whole for a short sample.
std::vector<bool> columnsNotSampled(const Block & block)
{
    std::vector<bool> not_sampled(block.columns());
    for (size_t i = 0; i < block.columns(); ++i)
        not_sampled[i] = hasAggregateFunctionType(block.getByPosition(i).type);
    return not_sampled;
}

/// Format one row of a block as an escaped tab-separated line, capped in length.
String formatRow(const Block & block, const std::vector<bool> & not_sampled, size_t row)
{
    static const FormatSettings format_settings;

    LineSampleWriteBuffer out;
    try
    {
        for (size_t i = 0; i < block.columns(); ++i)
        {
            if (i)
                writeChar('\t', out);
            if (not_sampled[i])
            {
                writeCString("[aggregate function state]", out);
                continue;
            }
            const auto & column_with_type = block.getByPosition(i);
            column_with_type.type->getDefaultSerialization()->serializeTextEscaped(*column_with_type.column, row, out, format_settings);
        }
        out.finalize();
    }
    catch (const LineSampleWriteBuffer::Full &) // NOLINT(bugprone-empty-catch)
    {
        /// The line is longer than the sample: what the buffer holds is its beginning.
    }
    return truncateForContext(String(out.sample()), QueryContextBuffer::max_line_bytes);
}

}

void QueryContextBuffer::startQuery(const String & query, bool from_ai)
{
    if (Entry * open = openEntry())
        open->finished = true;

    Entry entry;
    entry.seqno = next_seqno++;
    entry.query = truncateForContext(query, max_query_bytes);
    entry.from_ai = from_ai;
    entries.push_back(std::move(entry));

    while (entries.size() > max_entries)
        entries.pop_front();
}

void QueryContextBuffer::addBlock(const Block & block)
{
    Entry * entry = openEntry();
    if (!entry || block.columns() == 0)
        return;

    if (entry->header.empty())
    {
        WriteBufferFromOwnString out;
        for (size_t i = 0; i < block.columns(); ++i)
        {
            if (i)
                writeChar('\t', out);
            const auto & column_with_type = block.getByPosition(i);
            writeString(column_with_type.name, out);
            writeChar(':', out);
            writeString(column_with_type.type->getName(), out);
        }
        entry->header = truncateForContext(out.str(), max_line_bytes);
    }

    size_t rows = block.rows();
    if (rows == 0)
        return;

    /// Const and sparse columns cannot be serialized row by row directly.
    const Block materialized = materializeBlock(block);
    const std::vector<bool> not_sampled = columnsNotSampled(materialized);

    for (size_t row = 0; row < rows && entry->head_lines.size() < head_rows; ++row)
        entry->head_lines.push_back(formatRow(materialized, not_sampled, row));

    size_t tail_from = rows > tail_rows ? rows - tail_rows : 0;
    for (size_t row = tail_from; row < rows; ++row)
    {
        entry->tail_lines.push_back(formatRow(materialized, not_sampled, row));
        while (entry->tail_lines.size() > tail_rows)
            entry->tail_lines.pop_front();
    }

    entry->result_rows += rows;
}

void QueryContextBuffer::recordError(const String & fallback_query, const String & message, bool from_ai)
{
    if (Entry * entry = openEntry())
    {
        if (entry->error.empty())
            entry->error = truncateForContext(message, max_error_bytes);
        return;
    }

    /// The query failed before it was started (e.g. it could not be parsed):
    /// record it as a standalone, already finished entry.
    startQuery(fallback_query, from_ai);
    Entry & entry = entries.back();
    entry.error = truncateForContext(message, max_error_bytes);
    entry.finished = true;
}

void QueryContextBuffer::finishQuery(double elapsed_seconds, bool cancelled)
{
    if (Entry * entry = openEntry())
    {
        entry->elapsed_seconds = elapsed_seconds;
        entry->cancelled = cancelled;
        entry->finished = true;
    }
}

UInt64 QueryContextBuffer::latestSeqno() const
{
    return next_seqno - 1;
}

void QueryContextBuffer::dropFinishedEntries()
{
    std::erase_if(entries, [](const Entry & entry) { return entry.finished; });
}

String QueryContextBuffer::format(UInt64 since_seqno, bool skip_ai_initiated) const
{
    WriteBufferFromOwnString out;
    for (const auto & entry : entries)
    {
        if (entry.seqno <= since_seqno || (skip_ai_initiated && entry.from_ai))
            continue;
        formatEntry(entry, out);
    }
    return out.str();
}

void QueryContextBuffer::formatEntry(const Entry & entry, WriteBuffer & out)
{
    writeString("Query: ", out);
    writeString(entry.query, out);
    writeChar('\n', out);

    if (!entry.error.empty())
    {
        writeString("Error: ", out);
        writeString(entry.error, out);
        writeChar('\n', out);
    }
    else if (entry.cancelled)
    {
        writeString("Result: cancelled by the user\n", out);
    }
    else
    {
        writeString(fmt::format("Result: {} rows in {:.3f} sec.", entry.result_rows, entry.elapsed_seconds), out);
        writeChar('\n', out);

        if (!entry.header.empty() && entry.result_rows > 0)
        {
            writeString(entry.header, out);
            writeChar('\n', out);

            for (const auto & line : entry.head_lines)
            {
                writeString(line, out);
                writeChar('\n', out);
            }

            /// The tail sample overlaps the head sample when the result is small: print
            /// only the rows that were not shown yet, with an ellipsis for the gap.
            if (entry.result_rows > entry.head_lines.size())
            {
                size_t tail_overlap = 0;
                if (entry.head_lines.size() + entry.tail_lines.size() > entry.result_rows)
                    tail_overlap = entry.head_lines.size() + entry.tail_lines.size() - entry.result_rows;

                if (entry.result_rows > entry.head_lines.size() + (entry.tail_lines.size() - tail_overlap))
                    writeString("…\n", out);

                for (size_t i = tail_overlap; i < entry.tail_lines.size(); ++i)
                {
                    writeString(entry.tail_lines[i], out);
                    writeChar('\n', out);
                }
            }
        }
    }
    writeChar('\n', out);
}

QueryContextBuffer::Entry * QueryContextBuffer::openEntry()
{
    if (!entries.empty() && !entries.back().finished)
        return &entries.back();
    return nullptr;
}

String formatBlockAsTextForAI(const Block & block)
{
    static constexpr size_t max_rows = 200;
    static constexpr size_t max_bytes = 48 * 1024;

    WriteBufferFromOwnString out;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        if (i)
            writeChar('\t', out);
        const auto & column_with_type = block.getByPosition(i);
        writeString(column_with_type.name, out);
        writeChar(':', out);
        writeString(column_with_type.type->getName(), out);
    }
    writeChar('\n', out);

    if (block.rows() == 0)
        return out.str();

    const Block materialized = materializeBlock(block);
    const std::vector<bool> not_sampled = columnsNotSampled(materialized);
    size_t row = 0;
    for (; row < materialized.rows(); ++row)
    {
        if (row >= max_rows || out.count() > max_bytes)
        {
            writeString(fmt::format("(truncated: {} of {} rows shown)\n", row, materialized.rows()), out);
            break;
        }
        writeString(formatRow(materialized, not_sampled, row), out);
        writeChar('\n', out);
    }

    return out.str();
}

}
