#pragma once

#include <Interpreters/WindowDescription.h>

#include <Parsers/IAST.h>


namespace DB
{

struct ASTWindowDefinition : public IAST
{
    std::string parent_window_name;

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

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    std::string getDefaultWindowName() const;
};

struct ASTWindowListElement : public IAST
{
    String name;

    // ASTWindowDefinition
    ASTPtr definition;

    ASTPtr clone() const override;

    String getID(char delimiter) const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
