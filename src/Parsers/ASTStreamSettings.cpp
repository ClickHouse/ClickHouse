#include <Parsers/ASTStreamSettings.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

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
void formatNested(WriteBuffer & wb, CursorTreeNode * node)
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
            formatNested(wb, std::get<CursorTreeNodePtr>(v).get());
    }

    wb << '}';
}

}

ASTStreamSettings::ASTStreamSettings(StreamSettings settings_)
    : settings{std::move(settings_)}
{
}

void ASTStreamSettings::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    if (settings.cursor_tree.has_value())
    {
        auto tree = buildCursorTree(settings.cursor_tree.value());
        ostr << "CURSOR ";
        formatNested(ostr, tree.get());
    }
}

void ASTStreamSettings::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "StreamSettings");

    if (settings.cursor_tree.has_value())
        w.writeFieldValue("cursor_tree", Field(settings.cursor_tree.value()));
}

void ASTStreamSettings::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    if (r.has("cursor_tree"))
        settings.cursor_tree = r.readField("cursor_tree").safeGet<Map>();
}

}
