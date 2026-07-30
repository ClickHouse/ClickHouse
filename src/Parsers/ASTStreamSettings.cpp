#include <Parsers/ASTStreamSettings.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Core/Streaming/CursorTree.h>

#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

void formatCursorTree(WriteBuffer & wb, const CursorTreeNode * node)
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
    const WatermarkSettings & node,
    const IAST::FormatSettings & format_settings,
    IAST::FormatState & state,
    IAST::FormatStateStacked frame)
{
    wb << "FOR " << backQuoteIfNeed(node.column) << " AS ";
    node.expression->format(wb, format_settings, state, frame);

    if (node.idle_timeout.count() > 0)
        wb << " IDLE TIMEOUT INTERVAL " << static_cast<Int64>(node.idle_timeout.count()) << " MILLISECOND";
}

}

ASTPtr ASTStreamSettings::clone() const
{
    auto cloned_stream_settings = make_intrusive<ASTStreamSettings>();

    if (cursor)
        cloned_stream_settings->cursor = cursor->clone();
    if (watermark)
        cloned_stream_settings->watermark = watermark->clone();

    return cloned_stream_settings;
}

bool ASTStreamSettings::hasTweaks() const
{
    return cursor != nullptr || watermark != nullptr;
}

void ASTStreamSettings::formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const
{
    if (cursor)
    {
        ostr << "CURSOR ";
        formatCursorTree(ostr, cursor.get());
    }

    if (watermark)
    {
        if (cursor)
            ostr << ' ';

        ostr << "WATERMARK ";
        formatWatermark(ostr, *watermark, format_settings, state, frame);
    }
}

void ASTStreamSettings::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "StreamSettings");

    if (cursor)
        w.writeFieldValue("cursor_tree", Field(cursorTreeToMap(cursor)));

    if (watermark)
    {
        w.writeString("watermark_column", watermark->column);
        w.writeChild("watermark_expression", watermark->expression);
        w.writeInt("watermark_idle_timeout_ms", static_cast<Int64>(watermark->idle_timeout.count()));
    }
}

void ASTStreamSettings::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    if (r.has("cursor_tree"))
        cursor = buildCursorTree(r.readField("cursor_tree").safeGet<Map>());

    if (r.has("watermark_column"))
    {
        watermark = std::make_shared<WatermarkSettings>();
        watermark->column = r.getString("watermark_column");
        watermark->expression = r.readChild("watermark_expression");
        watermark->idle_timeout = std::chrono::milliseconds(r.getInt("watermark_idle_timeout_ms"));
    }
}

}
