#pragma once

#include <Parsers/ASTWithAlias.h>


namespace DB
{


/** SELECT subquery
  */
class ASTSubquery : public ASTWithAlias
{
public:
    /** Get the text that identifies this element. */
    String getID() const override { return "Subquery"; }

    ASTPtr clone() const override
    {
        const auto res = std::make_shared<ASTSubquery>(*this);
        ASTPtr ptr{res};

        res->children.clear();

        for (const auto & child : children)
            res->children.emplace_back(child->clone());

        return ptr;
    }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        settings.ostr << nl_or_nothing << indent_str << "(" << nl_or_nothing;
        FormatStateStacked frame_nested = frame;
        frame_nested.need_parens = false;
        ++frame_nested.indent;
        children[0]->formatImpl(settings, state, frame_nested);
        settings.ostr << nl_or_nothing << indent_str << ")";
    }

    String getColumnNameImpl() const override;
};

}
