#pragma once

#include <Parsers/IAST.h>
#include <Storages/DataDestinationType.h>
#include <Storages/TTLMode.h>

namespace Poco::JSON { class Object; }

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

    ASTTTLElement()
        : mode(TTLMode::DELETE)
        , destination_type(DataDestinationType::DELETE)
        , if_exists(false)
    {
    }

    ASTTTLElement(TTLMode mode_, DataDestinationType destination_type_, const String & destination_name_, bool if_exists_)
        : mode(mode_)
        , destination_type(destination_type_)
        , destination_name(destination_name_)
        , if_exists(if_exists_)
    {
    }

    String getID(char) const override { return "TTLElement"; }

    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    ASTPtr ttl() const { return getExpression(ttl_expr_pos); }
    ASTPtr where() const { return getExpression(where_expr_pos); }
    /// `ASTExpressionList` of the `GROUP BY` keys.
    ASTPtr groupByKey() const { return getExpression(group_by_key_pos); }
    /// `ASTExpressionList` of `ASTAssignment` after `GROUP BY ... SET`.
    ASTPtr groupByAssignments() const { return getExpression(group_by_assignments_pos); }
    ASTPtr recompressionCodec() const { return getExpression(recompression_codec_pos); }

    /// Passing null removes the child.
    void setTTL(ASTPtr && ast) { setExpression(ttl_expr_pos, std::move(ast)); }
    void setWhere(ASTPtr && ast) { setExpression(where_expr_pos, std::move(ast)); }
    void setGroupByKey(ASTPtr && ast) { setExpression(group_by_key_pos, std::move(ast)); }
    void setGroupByAssignments(ASTPtr && ast) { setExpression(group_by_assignments_pos, std::move(ast)); }
    void setRecompressionCodec(ASTPtr && ast) { setExpression(recompression_codec_pos, std::move(ast)); }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    /// Indexes into `children`, -1 when absent.
    int ttl_expr_pos = -1;
    int where_expr_pos = -1;
    int group_by_key_pos = -1;
    int group_by_assignments_pos = -1;
    int recompression_codec_pos = -1;

    void setExpression(int & pos, ASTPtr && ast);
    ASTPtr getExpression(int pos) const { return pos != -1 ? children[pos] : ASTPtr{}; }
};

}
