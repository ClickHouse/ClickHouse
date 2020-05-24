#pragma once

#include <Parsers/IAST.h>
#include <Storages/MergeTree/PartDestinationType.h>
#include <Storages/MergeTree/TTLMode.h>


namespace DB
{

/** Element of TTL expression.
  */
class ASTTTLElement : public IAST
{
public:
    TTLMode mode;
    PartDestinationType destination_type;
    String destination_name;

    ASTs group_by_key;
    std::vector<std::pair<String, ASTPtr>> group_by_aggregations;

    ASTTTLElement(TTLMode mode_, PartDestinationType destination_type_, const String & destination_name_)
        : mode(mode_)
        , destination_type(destination_type_)
        , destination_name(destination_name_)
        , ttl_expr_pos(-1)
        , where_expr_pos(-1)
    {
    }

    String getID(char) const override { return "TTLElement"; }

    ASTPtr clone() const override;

    const ASTPtr ttl() const { return getExpression(ttl_expr_pos); }
    const ASTPtr where() const { return getExpression(where_expr_pos); }

    void setTTL(ASTPtr && ast) { setExpression(ttl_expr_pos, std::forward<ASTPtr>(ast)); }
    void setWhere(ASTPtr && ast) { setExpression(where_expr_pos, std::forward<ASTPtr>(ast)); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    int ttl_expr_pos;
    int where_expr_pos;

private:
    void setExpression(int & pos, ASTPtr && ast);
    ASTPtr getExpression(int pos, bool clone = false) const;
};

}
