#include <Parsers/ASTStreamSettings.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Core/Streaming/CursorTree.h>

#include <Common/quoteString.h>

namespace DB
{

namespace
{

/// Renders a cursor tree as a SQL-compatible nested map literal:
///     {'partition_a': {'block_number': 10, 'block_offset': 20}}
void formatCursorTree(WriteBuffer & wb, CursorTreeNode * node)
{
    wb << '{';

    bool first = true;
    for (const auto & [k, v] : *node)
    {
        if (!first)
            wb << ", ";
        first = false;

        wb << quoteString(k) << ": ";

        if (std::holds_alternative<Int64>(v))
            wb << std::get<Int64>(v);
        else
            formatCursorTree(wb, std::get<CursorTreeNodePtr>(v).get());
    }

    wb << '}';
}

void formatWatermark(
    WriteBuffer & wb,
    const ASTStreamSettings::WatermarkSettings & node,
    const IAST::FormatSettings & format_settings,
    IAST::FormatState & state,
    IAST::FormatStateStacked frame)
{
    wb << "FOR " << backQuoteIfNeed(node.column) << " AS ";
    node.expression->format(wb, format_settings, state, frame);

    if (node.idle_timeout_ms > 0)
        wb << " IDLE TIMEOUT INTERVAL " << node.idle_timeout_ms << " MILLISECOND";
}

}

ASTPtr ASTStreamSettings::clone() const
{
    auto cloned_stream_settings = make_intrusive<ASTStreamSettings>();

    cloned_stream_settings->cursor = cursor;

    cloned_stream_settings->watermark = watermark;
    if (watermark && watermark->expression)
        cloned_stream_settings->watermark->expression = watermark->expression->clone();

    return cloned_stream_settings;
}

bool ASTStreamSettings::hasTweaks() const
{
    return cursor.has_value() || watermark.has_value();
}

void ASTStreamSettings::formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const
{
    if (cursor)
    {
        auto tree = buildCursorTree(cursor.value());
        ostr << "CURSOR ";
        formatCursorTree(ostr, tree.get());
    }

    if (watermark)
    {
        if (cursor)
            ostr << ' ';

        ostr << "WATERMARK ";
        formatWatermark(ostr, *watermark, format_settings, state, frame);
    }
}

}
