#pragma once

#include <Parsers/IAST.h>
#include <Storages/DataDestinationType.h>
#include <Storages/TTLMode.h>


namespace DB
{

/** Element of TTL expression.
  */
class ASTTTLElement : public IAST
{
public:
    TTLMode mode;
    DataDestinationType destination_type;
    String destination_name;
    bool if_exists = false;

    ASTList group_by_key;
    ASTList group_by_assignments;

    ASTPtr recompression_codec;

    ASTTTLElement(TTLMode mode_, DataDestinationType destination_type_, const String & destination_name_, bool if_exists_)
        : mode(mode_)
        , destination_type(destination_type_)
        , destination_name(destination_name_)
        , if_exists(if_exists_)
        , ttl_expr_pos(children.end())
        , where_expr_pos(children.end())
    {
    }

    String getID(char) const override { return "TTLElement"; }

    ASTPtr clone() const override;

    ASTPtr ttl() const { return getExpression(ttl_expr_pos); }
    ASTPtr where() const { return getExpression(where_expr_pos); }

    void setTTL(ASTPtr && ast) { setExpression(ttl_expr_pos, std::forward<ASTPtr>(ast)); }
    void setWhere(ASTPtr && ast) { setExpression(where_expr_pos, std::forward<ASTPtr>(ast)); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    ASTList::iterator ttl_expr_pos;
    ASTList::iterator where_expr_pos;

    void setExpression(ASTList::iterator & pos, ASTPtr && ast);
    ASTPtr getExpression(ASTList::iterator, bool clone = false) const;
};

}
