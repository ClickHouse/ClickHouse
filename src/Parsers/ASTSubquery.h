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

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Subquery"; }

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTSubquery>(*this);
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
