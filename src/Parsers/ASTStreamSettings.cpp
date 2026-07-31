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
    {
        Field field = r.readField("cursor_tree");
        if (field.getType() != Field::Types::Map)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`StreamSettings` 'cursor_tree' must be a Map during AST JSON deserialization");

        const auto & map = field.safeGet<Map>();
        for (const auto & element : map)
        {
            if (element.getType() != Field::Types::Tuple)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "`StreamSettings` 'cursor_tree' element must be a tuple during AST JSON deserialization");

            const auto & tuple = element.safeGet<Tuple>();
            if (tuple.size() != 2 || tuple.at(0).getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "`StreamSettings` 'cursor_tree' element must be a (String, integer) tuple during AST JSON deserialization");

            const auto & value = tuple.at(1);
            if (value.getType() != Field::Types::UInt64 && value.getType() != Field::Types::Int64)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "`StreamSettings` 'cursor_tree' element must be a (String, integer) tuple during AST JSON deserialization");

            if (value.getType() == Field::Types::UInt64 && value.safeGet<UInt64>() > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "`StreamSettings` 'cursor_tree' value is out of Int64 range during AST JSON deserialization");
        }

        cursor = buildCursorTree(map);
    }

    if (r.has("watermark_column"))
    {
        auto column = r.getString("watermark_column");
        if (column.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`StreamSettings` 'watermark_column' must be a non-empty string during AST JSON deserialization");

        auto expression = r.readChild("watermark_expression");
        if (!expression)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`StreamSettings` 'watermark_expression' must be present when 'watermark_column' is set during AST JSON deserialization");

        auto idle_timeout_ms = r.getInt("watermark_idle_timeout_ms");
        if (idle_timeout_ms < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`StreamSettings` 'watermark_idle_timeout_ms' must be non-negative during AST JSON deserialization");

        watermark = std::make_shared<WatermarkSettings>();
        watermark->column = std::move(column);
        watermark->expression = std::move(expression);
        watermark->idle_timeout = std::chrono::milliseconds(idle_timeout_ms);
    }
}

}
