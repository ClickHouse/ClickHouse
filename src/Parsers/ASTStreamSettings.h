#pragma once

#include <Core/Streaming/CursorTree.h>
#include <Core/Streaming/ReadingStage.h>

#include <Parsers/IAST.h>

namespace DB
{


/// TODO
class ASTStreamSettings : public IAST
{
public:
    StreamReadingStage stage;
    std::optional<String> keeper_key;
    std::optional<Map> collapsed_tree;

    ASTStreamSettings(StreamReadingStage stage_, std::optional<String> keeper_key_, std::optional<Map> collapsed_tree_);

    String getID(char) const override { return "ASTStreamSettings"; }

    ASTPtr clone() const override { return std::make_shared<ASTStreamSettings>(*this); }

    void formatImpl(const FormatSettings & format, FormatState & state, FormatStateStacked frame) const override;
};

}
