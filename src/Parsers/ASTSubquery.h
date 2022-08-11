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
    // WITH (SELECT 1) AS a SELECT * FROM a AS b; cte_name will be `a`.
    std::string cte_name;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Subquery"; }

    ASTPtr clone() const override
    {
        const auto res = std::make_shared<ASTSubquery>(*this);
        ASTPtr ptr{res};

        res->children.clear();

        for (const auto & child : children)
            res->children.emplace_back(child->clone());

        return ptr;
    }

    void updateTreeHashImpl(SipHash & hash_state) const override;
    IAST::Hash getContentHash() const;
    String getAliasOrColumnName() const override;
    String tryGetAlias() const override;

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
