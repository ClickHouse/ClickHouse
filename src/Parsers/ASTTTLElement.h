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

    ASTs group_by_key;
    ASTs group_by_assignments;

    ASTPtr recompression_codec;

    ASTTTLElement(TTLMode mode_, DataDestinationType destination_type_, const String & destination_name_, bool if_exists_)
        : mode(mode_)
        , destination_type(destination_type_)
        , destination_name(destination_name_)
        , if_exists(if_exists_)
        , ttl_expr_pos(-1)
        , where_expr_pos(-1)
    {
    }

    String getID(char) const override { return "TTLElement"; }

    ASTPtr clone() const override;

    ASTPtr ttl() const { return getExpression(ttl_expr_pos); }
    ASTPtr where() const { return getExpression(where_expr_pos); }

    void setTTL(ASTPtr && ast) { setExpression(ttl_expr_pos, std::forward<ASTPtr>(ast)); }
    void setWhere(ASTPtr && ast) { setExpression(where_expr_pos, std::forward<ASTPtr>(ast)); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    int ttl_expr_pos;
    int where_expr_pos;

    void setExpression(int & pos, ASTPtr && ast);
    ASTPtr getExpression(int pos, bool clone = false) const;
};

}
