#include <Client/AI/QueryContextBuffer.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace DB
{

namespace
{

String truncateForContext(const String & text, size_t max_bytes)
{
    if (text.size() <= max_bytes)
        return text;
    return text.substr(0, max_bytes) + "…";
}

/// Format one row of a block as an escaped tab-separated line, capped in length.
String formatRow(const Block & block, size_t row)
{
    static const FormatSettings format_settings;

    WriteBufferFromOwnString out;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        if (i)
            writeChar('\t', out);
        const auto & column_with_type = block.getByPosition(i);
        column_with_type.type->getDefaultSerialization()->serializeTextEscaped(*column_with_type.column, row, out, format_settings);
        if (out.count() > QueryContextBuffer::max_line_bytes)
            break;
    }
    return truncateForContext(out.str(), QueryContextBuffer::max_line_bytes);
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

    for (size_t row = 0; row < rows && entry->head_lines.size() < head_rows; ++row)
        entry->head_lines.push_back(formatRow(materialized, row));

    size_t tail_from = rows > tail_rows ? rows - tail_rows : 0;
    for (size_t row = tail_from; row < rows; ++row)
    {
        entry->tail_lines.push_back(formatRow(materialized, row));
        while (entry->tail_lines.size() > tail_rows)
            entry->tail_lines.pop_front();
    }

    entry->result_rows += rows;
}

void QueryContextBuffer::recordError(const String & fallback_query, const String & message)
{
    if (Entry * entry = openEntry())
    {
        if (entry->error.empty())
            entry->error = truncateForContext(message, max_error_bytes);
        return;
    }

    /// The query failed before it was started (e.g. it could not be parsed):
    /// record it as a standalone, already finished entry.
    startQuery(fallback_query, /*from_ai=*/ false);
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
    return entries.empty() ? 0 : entries.back().seqno;
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
    size_t row = 0;
    for (; row < materialized.rows(); ++row)
    {
        if (row >= max_rows || out.count() > max_bytes)
        {
            writeString(fmt::format("(truncated: {} of {} rows shown)\n", row, materialized.rows()), out);
            break;
        }
        writeString(formatRow(materialized, row), out);
        writeChar('\n', out);
    }

    return out.str();
}

}
