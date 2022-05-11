#pragma once

#include <vector>
#include <Parsers/IAST.h>

namespace DB
{
class ASTJSONPathRange : public IAST
{
public:
    String getID(char) const override { return "ASTJSONPathRange"; }

    ASTPtr clone() const override { return std::make_shared<ASTJSONPathRange>(*this); }

public:
    /// Ranges to lookup in json array ($[0, 1, 2, 4 to 9])
    /// Range is represented as <start, end (non-inclusive)>
    /// Single index is represented as <start, start + 1>
    std::vector<std::pair<UInt32, UInt32>> ranges;
    bool is_star = false;
};

}
