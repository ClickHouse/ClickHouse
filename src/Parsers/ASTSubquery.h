#pragma once

#include <Parsers/ASTWithAlias.h>


namespace DB
{


/** SELECT subquery
  */
class ASTSubquery : public ASTWithAlias
{
public:
    // Stored the name when the subquery is defined in WITH clause. For example:
    // WITH a AS (SELECT 1) SELECT * FROM a AS b; cte_name will be `a`.
    String cte_name;

    /// Set when this subquery replaced a reference to an element of a `WITH RECURSIVE` list, so that the copy
    /// still binds its own self-reference: a substituted copy reaches the query tree builder in FROM position,
    /// where nothing else records that its list was recursive. A WITH element is marked from its own list
    /// instead, with this flag unset. Never set without `cte_name`, which names the table binding uses.
    bool recursive_with = false;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Subquery"; }

    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    ASTPtr clone() const override
    {
        auto clone = make_intrusive<ASTSubquery>(*this);
        clone->cloneChildren();
        return clone;
    }

    ASTSubquery() = default;

    explicit ASTSubquery(ASTPtr child)
    {
        children.emplace_back(std::move(child));
    }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    String getAliasOrColumnName() const override;
    String tryGetAlias() const override;

protected:
    void formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
