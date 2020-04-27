#pragma once

#include <Parsers/IAST.h>
#include <Storages/MergeTree/PartDestinationType.h>


namespace DB
{

/** Element of TTL expression.
  */
class ASTTTLElement : public IAST
{
public:
    enum class Expression : uint8_t
    {
        TTL,
        WHERE
    };

    enum class Mode : uint8_t
    {
        DELETE,
        MOVE,
        GROUP_BY
    };

    Mode mode;
    PartDestinationType destination_type;
    String destination_name;
    std::vector<String> group_by_key_columns;
    std::vector<std::pair<String, ASTPtr>> group_by_aggregations;

    ASTTTLElement(Mode mode_, PartDestinationType destination_type_, const String & destination_name_)
        : mode(mode_) 
        , destination_type(destination_type_)
        , destination_name(destination_name_)
    {
    }

    String getID(char) const override { return "TTLElement"; }

    ASTPtr clone() const override;

    const ASTPtr ttl() const { return getExpression(Expression::TTL); }
    const ASTPtr where() const { return getExpression(Expression::WHERE); }

    void setExpression(Expression expr, ASTPtr && ast);
    ASTPtr getExpression(Expression expr, bool clone = false) const;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    std::unordered_map<Expression, size_t> positions;
};

}
