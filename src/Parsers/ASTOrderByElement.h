#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/** Element of expression with ASC or DESC,
  *  and possibly with COLLATE.
  */
class ASTOrderByElement : public IAST
{
private:
    enum class Child : uint8_t
    {
        EXPRESSION,
        COLLATION,
        FILL_FROM,
        FILL_TO,
        FILL_STEP,
    };

public:
    int direction = 0; /// 1 for ASC, -1 for DESC
    int nulls_direction = 0; /// Same as direction for NULLS LAST, opposite for NULLS FIRST.
    bool nulls_direction_was_explicitly_specified = false;

    bool with_fill = false;

    /** Collation for locale-specific string comparison. If empty, then sorting done by bytes. */
    void setCollation(ASTPtr node) { setChild(Child::COLLATION, node); }
    void setFillFrom(ASTPtr node)  { setChild(Child::FILL_FROM, node); }
    void setFillTo(ASTPtr node)    { setChild(Child::FILL_TO, node);   }
    void setFillStep(ASTPtr node)  { setChild(Child::FILL_STEP, node); }

    /** Collation for locale-specific string comparison. If empty, then sorting done by bytes. */
    ASTPtr getCollation() const { return getChild(Child::COLLATION); }
    ASTPtr getFillFrom()  const { return getChild(Child::FILL_FROM); }
    ASTPtr getFillTo()    const { return getChild(Child::FILL_TO);   }
    ASTPtr getFillStep()  const { return getChild(Child::FILL_STEP); }

    String getID(char) const override { return "OrderByElement"; }

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTOrderByElement>(*this);
        clone->cloneChildren();
        return clone;
    }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
private:

    ASTPtr getChild(Child child) const
    {
        auto it = positions.find(child);
        if (it != positions.end())
            return children[it->second];
        return {};
    }

    void setChild(Child child, ASTPtr node)
    {
        if (node == nullptr)
            return;

        auto it = positions.find(child);
        if (it != positions.end())
        {
            children[it->second] = node;
        }
        else
        {
            positions[child] = children.size();
            children.push_back(node);
        }
    }

    std::unordered_map<Child, size_t> positions;
};

}
