#include <Parsers/ASTStreamSettings.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Core/Streaming/CursorTree.h>

#include <Common/quoteString.h>
#include <Common/SipHash.h>

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

void updateCursorTreeHash(SipHash & hash_state, const CursorTreeNode & node)
{
    for (const auto & [key, value] : node)
    {
        hash_state.update(key.size());
        hash_state.update(key);
        if (const auto * number = std::get_if<Int64>(&value))
        {
            hash_state.update(static_cast<UInt8>(0));
            hash_state.update(*number);
        }
        else
        {
            hash_state.update(static_cast<UInt8>(1));
            updateCursorTreeHash(hash_state, *std::get<CursorTreeNodePtr>(value));
        }
    }
    /// Delimit the node, otherwise a nested subtree and its flattened-out sibling entries hash equally.
    hash_state.update(static_cast<UInt8>(2));
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

    cloned_stream_settings->setSubscribeForUpdates(subscribe_for_updates);
    cloned_stream_settings->setUnordered(unordered);
    if (cursor)
        cloned_stream_settings->setCursor(cursor->clone());
    if (watermark)
        cloned_stream_settings->setWatermark(watermark->clone());

    return cloned_stream_settings;
}

void ASTStreamSettings::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// The cursor tree, the watermark column/idle timeout and the two modifier flags are not
    /// children (only the watermark expression is, see `setWatermark`), so hash them explicitly.
    /// The expected size is for 64-bit targets; the layout differs on 32-bit ones (the wasm parser build).
    static_assert(sizeof(void *) != 8 || sizeof(*this) == 72, "If members were added to ASTStreamSettings, hash them here unless they are purely cosmetic.");
    hash_state.update(subscribe_for_updates);
    hash_state.update(unordered);
    hash_state.update(cursor != nullptr);
    if (cursor)
        updateCursorTreeHash(hash_state, *cursor);
    hash_state.update(watermark != nullptr);
    if (watermark)
    {
        hash_state.update(watermark->column.size());
        hash_state.update(watermark->column);
        hash_state.update(watermark->idle_timeout.count());
    }
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

bool ASTStreamSettings::hasTweaks() const
{
    return !subscribe_for_updates || unordered || cursor != nullptr || watermark != nullptr;
}

void ASTStreamSettings::setSubscribeForUpdates(bool subscribe_for_updates_)
{
    subscribe_for_updates = subscribe_for_updates_;
}

void ASTStreamSettings::setUnordered(bool unordered_)
{
    unordered = unordered_;
}

void ASTStreamSettings::setCursor(CursorTreeNodePtr cursor_)
{
    cursor = std::move(cursor_);
}

void ASTStreamSettings::setWatermark(WatermarkSettingsPtr watermark_)
{
    watermark = std::move(watermark_);
    children.push_back(watermark->expression);
}

void ASTStreamSettings::formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const
{
    bool need_space = false;

    if (!subscribe_for_updates)
    {
        ostr << "BOUNDED";
        need_space = true;
    }

    if (unordered)
    {
        if (need_space)
            ostr << ' ';
        ostr << "UNORDERED";
        need_space = true;
    }

    if (cursor)
    {
        if (need_space)
            ostr << ' ';
        ostr << "CURSOR ";
        formatCursorTree(ostr, cursor.get());
        need_space = true;
    }

    if (watermark)
    {
        if (need_space)
            ostr << ' ';
        ostr << "WATERMARK ";
        formatWatermark(ostr, *watermark, format_settings, state, frame);
    }
}

void ASTStreamSettings::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    if (watermark)
        f(nullptr, &watermark->expression);
}

void ASTStreamSettings::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "StreamSettings");

    if (!subscribe_for_updates)
        w.writeInt("subscribe_for_updates", 0);

    if (unordered)
        w.writeInt("unordered", 1);

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

    if (r.has("subscribe_for_updates"))
        setSubscribeForUpdates(r.getInt("subscribe_for_updates") != 0);

    if (r.has("unordered"))
        setUnordered(r.getInt("unordered") != 0);

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

        setCursor(buildCursorTree(map));
    }

    /// The parser produces the three watermark fields only together, as one
    /// `WATERMARK FOR <col> AS <expr> ...` clause, and `writeJSON` emits them together too. Orphaned
    /// fields without 'watermark_column' would be silently dropped here, so the query would hash,
    /// format, and execute as if no watermark existed - reject them as parser-impossible.
    if (!r.has("watermark_column") && (r.has("watermark_expression") || r.has("watermark_idle_timeout_ms")))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`StreamSettings` 'watermark_expression' and 'watermark_idle_timeout_ms' require 'watermark_column' during AST JSON deserialization");

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

        auto new_watermark = std::make_shared<WatermarkSettings>();
        new_watermark->column = std::move(column);
        new_watermark->expression = std::move(expression);
        new_watermark->idle_timeout = std::chrono::milliseconds(idle_timeout_ms);
        setWatermark(std::move(new_watermark));
    }
}

}
