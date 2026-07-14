#pragma once

#include <Core/IdentifierName.h>

#include <Interpreters/WindowDescription.h>

#include <Parsers/IAST.h>


namespace DB
{

struct ASTWindowDefinition : public IAST
{
    std::string parent_window_name;
    /// Quoting of the parent window name as written in the query. Double quotes pin the name
    /// to exact-case matching under `standard` name matching.
    IdentifierPartQuote parent_window_name_quote = IdentifierPartQuote::Unquoted;

    ASTPtr partition_by;

    ASTPtr order_by;

    bool frame_is_default = true;
    WindowFrame::FrameType frame_type = WindowFrame::FrameType::RANGE;
    WindowFrame::BoundaryType frame_begin_type = WindowFrame::BoundaryType::Unbounded;
    ASTPtr frame_begin_offset;
    bool frame_begin_preceding = true;
    WindowFrame::BoundaryType frame_end_type = WindowFrame::BoundaryType::Current;
    ASTPtr frame_end_offset;
    bool frame_end_preceding = false;

    ASTPtr clone() const override;

    String getID(char delimiter) const override;

    std::string getDefaultWindowName() const;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(nullptr, &partition_by);
        f(nullptr, &order_by);
        f(nullptr, &frame_begin_offset);
        f(nullptr, &frame_end_offset);
    }


protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

struct ASTWindowListElement : public IAST
{
    String name;
    /// Quoting of the window name as written in the query.
    IdentifierPartQuote name_quote = IdentifierPartQuote::Unquoted;

    // ASTWindowDefinition
    ASTPtr definition;

    ASTPtr clone() const override;

    String getID(char delimiter) const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
