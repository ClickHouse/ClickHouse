#include <Common/quoteString.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTWithAlias.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <Poco/JSON/Array.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ASTPtr ASTTTLElement::clone() const
{
    auto clone = make_intrusive<ASTTTLElement>(*this);
    clone->children.clear();
    clone->ttl_expr_pos = -1;
    clone->where_expr_pos = -1;

    clone->setExpression(clone->ttl_expr_pos, getExpression(ttl_expr_pos, true));
    clone->setExpression(clone->where_expr_pos, getExpression(where_expr_pos, true));

    for (auto & expr : clone->group_by_key)
        expr = expr->clone();
    for (auto & expr : clone->group_by_assignments)
        expr = expr->clone();

    return clone;
}

void ASTTTLElement::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "TTLElement");
    w.writeString("mode", std::string(magic_enum::enum_name(mode)));
    w.writeString("destination_type", std::string(magic_enum::enum_name(destination_type)));

    if (!destination_name.empty())
        w.writeString("destination_name", destination_name);

    w.writeBool("if_exists", if_exists);
    w.writeChild("ttl_expr", ttl());
    w.writeChild("where_expr", where());

    if (!group_by_key.empty())
    {
        w.writeKey("group_by_key");
        w.writeArray(group_by_key);
    }

    if (!group_by_assignments.empty())
    {
        w.writeKey("group_by_assignments");
        w.writeArray(group_by_assignments);
    }

    w.writeChild("recompression_codec", recompression_codec);
}

void ASTTTLElement::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    String mode_str = r.getString("mode");
    auto mode_opt = magic_enum::enum_cast<TTLMode>(mode_str);
    if (mode_opt)
        mode = *mode_opt;

    String dest_type_str = r.getString("destination_type");
    auto dest_opt = magic_enum::enum_cast<DataDestinationType>(dest_type_str);
    if (dest_opt)
        destination_type = *dest_opt;

    destination_name = r.getString("destination_name");
    if_exists = r.getBool("if_exists");

    auto ttl_child = r.readChild("ttl_expr");
    if (ttl_child)
        setTTL(std::move(ttl_child));

    auto where_child = r.readChild("where_expr");
    if (where_child)
        setWhere(std::move(where_child));

    auto arr = r.getArray("group_by_key");
    if (arr)
    {
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto child_obj = arr->getObject(i);
            group_by_key.push_back(IAST::createFromJSON(*child_obj));
        }
    }

    arr = r.getArray("group_by_assignments");
    if (arr)
    {
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto child_obj = arr->getObject(i);
            group_by_assignments.push_back(IAST::createFromJSON(*child_obj));
        }
    }

    recompression_codec = r.readChild("recompression_codec");
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
        for (auto it = group_by_key.begin(); it != group_by_key.end(); ++it)
        {
            if (it != group_by_key.begin())
                ostr << ", ";
            (*it)->format(ostr, settings, state, frame);
        }

        if (!group_by_assignments.empty())
        {
            ostr << " SET ";
            for (auto it = group_by_assignments.begin(); it != group_by_assignments.end(); ++it)
            {
                if (it != group_by_assignments.begin())
                    ostr << ", ";
                (*it)->format(ostr, settings, state, frame);
            }
        }
    }
    else if (mode == TTLMode::RECOMPRESS)
    {
        ostr << " RECOMPRESS ";
        recompression_codec->format(ostr, settings, state, frame);
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
            children.emplace_back(ast);
        }
        else
            children[pos] = ast;
    }
    else if (pos != -1)
    {
        children[pos] = ASTPtr{};
        pos = -1;
    }
}

ASTPtr ASTTTLElement::getExpression(int  pos, bool clone) const
{
    return pos != -1 ? (clone ? children[pos]->clone() : children[pos]) : ASTPtr{};
}

}
