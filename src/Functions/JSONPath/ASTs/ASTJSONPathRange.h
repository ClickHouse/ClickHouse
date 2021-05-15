#pragma once

#include <Parsers/IAST.h>
#include <vector>

namespace DB
{

class ASTJSONPathRange : public IAST
{
public:
    String getID(char) const override
    {
        return "ASTJSONPathRange";
    }

    ASTPtr clone() const override
    {
        return std::make_shared<ASTJSONPathRange>(*this);
    }

public:
    /// Ranges to lookup in json array ($[0, 1, 2, 4 to 9])
    /// Range is represented as <start, end>
    /// Single index is represented as <start, start>
    std::vector<std::pair<UInt32, UInt32>> ranges;
    bool is_star = false;
};

}
