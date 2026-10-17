#pragma once

#include <vector>
#include <Parsers/IAST.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{
class ASTJSONPathRange : public IAST
{
public:
    String getID(char) const override { return "ASTJSONPathRange"; }

    ASTPtr clone() const override { return make_intrusive<ASTJSONPathRange>(*this); }

    /// Ranges to lookup in json array ($[0, 1, 2, 4 to 9])
    /// Range is represented as <first, last>, both inclusive
    /// Single index is represented as <index, index>
    VectorWithMemoryTracking<std::pair<UInt64, UInt64>> ranges;
    bool is_star = false;
};

}
