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
    struct StreamSettings {
        StreamReadingStage stage = StreamReadingStage::AllData;
        std::optional<String> keeper_key;
        std::optional<Map> collapsed_tree;
    };

    StreamSettings settings;

    explicit ASTStreamSettings(StreamSettings settings_);

    String getID(char) const override { return "ASTStreamSettings"; }

    ASTPtr clone() const override { return std::make_shared<ASTStreamSettings>(*this); }

    void formatImpl(const FormatSettings & format, FormatState & state, FormatStateStacked frame) const override;
};

}
