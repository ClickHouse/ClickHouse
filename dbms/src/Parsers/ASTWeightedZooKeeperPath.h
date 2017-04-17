#pragma once

#include <Parsers/IAST.h>
#include <Core/Types.h>
#include <iomanip>


namespace DB
{

class ASTWeightedZooKeeperPath : public IAST
{
public:
    ASTWeightedZooKeeperPath() = default;
    ASTWeightedZooKeeperPath(StringRange range_) : IAST(range_) {}
    String getID() const override { return "Weighted_ZooKeeper_Path"; }
    ASTPtr clone() const override { return std::make_shared<ASTWeightedZooKeeperPath>(*this); }

public:
    String path;
    UInt64 weight;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        settings.ostr << settings.nl_or_ws << indent_str << std::quoted(path, '\'') << " WEIGHT " << weight;
    }
};

}
