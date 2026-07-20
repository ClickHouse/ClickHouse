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
    {
        /// `ParserStreamSettings` produces `cursor_tree` only as a flat `Map` of
        /// `(String dotted path, unsigned integer leaf)` tuples, and `buildCursorTree` (reached from
        /// `formatImpl` and `QueryTreeBuilder`) relies on that shape via `safeGet`. Validate every
        /// element here so malformed `clickhouse_json` fails with `BAD_ARGUMENTS` at the boundary
        /// instead of inside formatter/analyzer code. The leaf may be `UInt64` (parser-produced) or
        /// `Int64` (`cursorTreeToMap`-produced, see `Analyzer/Utils.cpp`); both satisfy `safeGet<Int64>`.
        Field field = r.readField("cursor_tree");
        if (field.getType() != Field::Types::Map)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`StreamSettings` 'cursor_tree' must be a Map during AST JSON deserialization");

        Map map = std::move(field.safeGet<Map>());
        for (const auto & element : map)
        {
            if (element.getType() != Field::Types::Tuple)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "`StreamSettings` 'cursor_tree' element must be a tuple during AST JSON deserialization");

            const auto & tuple = element.safeGet<Tuple>();
            if (tuple.size() != 2
                || tuple.at(0).getType() != Field::Types::String
                || (tuple.at(1).getType() != Field::Types::UInt64 && tuple.at(1).getType() != Field::Types::Int64))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "`StreamSettings` 'cursor_tree' element must be a (String, integer) tuple during AST JSON deserialization");
        }
        settings.cursor_tree = std::move(map);
    }
}

}
