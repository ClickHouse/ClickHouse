#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTCodec.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <Poco/JSON/Array.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

ASTPtr ASTTTLElement::clone() const
{
    auto clone = make_intrusive<ASTTTLElement>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTTTLElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    static_assert(sizeof(void *) != 8 || sizeof(*this) == 88, "If members were added to ASTTTLElement, hash them here unless they are purely cosmetic.");
    hash_state.update(mode);
    hash_state.update(destination_type);
    hash_state.update(destination_name.size());
    hash_state.update(destination_name);
    hash_state.update(if_exists);
    /// Which slot a child occupies is part of the meaning, not only its content.
    for (int pos : {ttl_expr_pos, where_expr_pos, group_by_key_pos, group_by_assignments_pos, recompression_codec_pos})
        hash_state.update(pos);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTTTLElement::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "TTLElement");
    w.writeString("mode", magic_enum::enum_name(mode));
    w.writeString("destination_type", magic_enum::enum_name(destination_type));

    if (!destination_name.empty())
        w.writeString("destination_name", destination_name);

    w.writeBool("if_exists", if_exists);
    w.writeChild("ttl_expr", ttl());
    w.writeChild("where_expr", where());

    if (auto keys = groupByKey(); keys && !keys->children.empty())
    {
        w.writeKey("group_by_key");
        w.writeArray(keys->children);
    }

    if (auto assignments = groupByAssignments(); assignments && !assignments->children.empty())
    {
        w.writeKey("group_by_assignments");
        w.writeArray(assignments->children);
    }

    w.writeChild("recompression_codec", recompressionCodec());
}

void ASTTTLElement::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    String mode_str = r.getString("mode");
    if (auto mode_opt = magic_enum::enum_cast<TTLMode>(mode_str))
        mode = *mode_opt;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown 'mode' value in JSON AST for TTLElement: '{}'", mode_str);

    String dest_type_str = r.getString("destination_type");
    if (auto dest_opt = magic_enum::enum_cast<DataDestinationType>(dest_type_str))
        destination_type = *dest_opt;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown 'destination_type' value in JSON AST for TTLElement: '{}'", dest_type_str);

    destination_name = r.getString("destination_name");
    if_exists = r.getBool("if_exists");

    auto ttl_child = r.readExpressionChild("ttl_expr");
    if (!ttl_child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Required field 'ttl_expr' is missing in JSON AST for TTLElement");
    setTTL(std::move(ttl_child));

    auto where_child = r.readExpressionChild("where_expr");
    if (where_child)
        setWhere(std::move(where_child));

    /// An empty array leaves the slot absent, so the mode checks below treat it like a missing key.
    if (auto arr = r.getArray("group_by_key"); arr && arr->size() > 0)
    {
        auto keys = make_intrusive<ASTExpressionList>();
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto child_obj = arr->getObject(i);
            if (!child_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'group_by_key' array during AST JSON deserialization", i);
            keys->children.push_back(IAST::createFromJSON(*child_obj));
        }
        setGroupByKey(std::move(keys));
    }

    if (auto arr = r.getArray("group_by_assignments"); arr && arr->size() > 0)
    {
        auto assignments = make_intrusive<ASTExpressionList>();
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto child_obj = arr->getObject(i);
            if (!child_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'group_by_assignments' array during AST JSON deserialization", i);
            auto assignment = IAST::createFromJSON(*child_obj);
            /// `TTLDescription::getTTLFromAST` does `ast->as<const ASTAssignment &>()` for each element.
            if (!assignment || !assignment->as<ASTAssignment>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Element at index {} in 'group_by_assignments' must be an assignment during AST JSON deserialization", i);
            assignments->children.push_back(std::move(assignment));
        }
        setGroupByAssignments(std::move(assignments));
    }

    /// Only `RECOMPRESS` carries a codec; `formatImpl` drops it for every other mode, so a codec on a
    /// non-`RECOMPRESS` element would be silently lost.
    setRecompressionCodec(r.readChildOfType<ASTCodec>("recompression_codec"));
    if (mode == TTLMode::RECOMPRESS && !recompressionCodec())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Required field 'recompression_codec' is missing for RECOMPRESS mode during AST JSON deserialization");
    if (mode != TTLMode::RECOMPRESS && recompressionCodec())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'recompression_codec' is only valid for TTL RECOMPRESS during AST JSON deserialization");

    /// Only `TTL ... TO DISK/VOLUME '<name>'` (mode `MOVE`) carries a destination. `ParserTTLElement`
    /// produces exactly `DISK` or `VOLUME` with a non-empty name there; every other mode leaves the
    /// destination at its default (`DELETE`, no name). Reject the parser-impossible shapes a malformed
    /// `clickhouse_json` could carry: a non-`DISK`/`VOLUME` destination for `MOVE` (which would reach the
    /// `LOGICAL_ERROR` branch in `formatImpl`), an empty `MOVE` destination name (mirrors the sibling
    /// `MOVE_PARTITION` check in `ASTAlterCommand`), or any destination state on a non-`MOVE` mode.
    if (mode == TTLMode::MOVE)
    {
        if (destination_type != DataDestinationType::DISK && destination_type != DataDestinationType::VOLUME)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "TTL MOVE requires a DISK or VOLUME 'destination_type' during AST JSON deserialization");
        if (destination_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "TTL MOVE requires a non-empty 'destination_name' during AST JSON deserialization");
    }
    else if (destination_type != DataDestinationType::DELETE || !destination_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'destination_type'/'destination_name' are only valid for TTL MOVE during AST JSON deserialization");

    /// `GROUP BY` is the only mode that carries `group_by_key`/`group_by_assignments`, and
    /// `ParserTTLElement` sets it only after parsing at least one grouping key. Reject the
    /// parser-impossible shapes: a `GROUP_BY` without keys (`formatImpl` would emit `GROUP BY ` with no
    /// expressions) or these fields on `DELETE`/`MOVE`/`RECOMPRESS` (where `formatImpl` drops them).
    if (mode == TTLMode::GROUP_BY)
    {
        if (!groupByKey())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "TTL GROUP BY requires a non-empty 'group_by_key' during AST JSON deserialization");
    }
    else if (groupByKey() || groupByAssignments())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'group_by_key'/'group_by_assignments' are only valid for TTL GROUP BY during AST JSON deserialization");
}

void ASTTTLElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    auto ttl_expr = ttl();
    auto nested_frame = frame;
    if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(ttl_expr.get()); ast_alias && !ast_alias->tryGetAlias().empty())
        nested_frame.need_parens = true;
    ttl_expr->format(ostr, settings, state, nested_frame);
    if (mode == TTLMode::MOVE)
    {
        if (destination_type == DataDestinationType::DISK)
            ostr << " TO DISK ";
        else if (destination_type == DataDestinationType::VOLUME)
            ostr << " TO VOLUME ";
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unsupported destination type {} for TTL MOVE",
                    magic_enum::enum_name(destination_type));

        if (if_exists)
            ostr << "IF EXISTS ";

        ostr << quoteString(destination_name);
    }
    else if (mode == TTLMode::GROUP_BY)
    {
        ostr << " GROUP BY ";
        if (auto keys = groupByKey())
        {
            for (auto it = keys->children.begin(); it != keys->children.end(); ++it)
            {
                if (it != keys->children.begin())
                    ostr << ", ";
                (*it)->format(ostr, settings, state, frame);
            }
        }

        if (auto assignments = groupByAssignments(); assignments && !assignments->children.empty())
        {
            ostr << " SET ";
            for (auto it = assignments->children.begin(); it != assignments->children.end(); ++it)
            {
                if (it != assignments->children.begin())
                    ostr << ", ";
                (*it)->format(ostr, settings, state, frame);
            }
        }
    }
    else if (mode == TTLMode::RECOMPRESS)
    {
        ostr << " RECOMPRESS ";
        recompressionCodec()->format(ostr, settings, state, frame);
    }
    else if (mode == TTLMode::DELETE)
    {
        /// It would be better to output "DELETE" here but that will break compatibility with earlier versions.
    }

    if (auto where_expr = where())
    {
        ostr << " WHERE ";
        auto where_frame = frame;
        if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(where_expr.get()); ast_alias && !ast_alias->tryGetAlias().empty())
            where_frame.need_parens = true;
        where_expr->format(ostr, settings, state, where_frame);
    }
}

void ASTTTLElement::setExpression(int & pos, ASTPtr && ast)
{
    if (ast)
    {
        if (pos == -1)
        {
            pos = static_cast<int>(children.size());
            children.emplace_back(std::move(ast));
        }
        else
            children[pos] = std::move(ast);
        return;
    }

    if (pos == -1)
        return;

    /// Erase instead of leaving a null child, which the generic tree walks do not expect.
    children.erase(children.begin() + pos);
    for (int * other : {&ttl_expr_pos, &where_expr_pos, &group_by_key_pos, &group_by_assignments_pos, &recompression_codec_pos})
    {
        if (*other > pos)
            --*other;
    }
    pos = -1;
}

}
