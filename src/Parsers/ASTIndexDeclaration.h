#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTFunction;

/** name BY expr TYPE typename(args) GRANULARITY int in create query
  */
class ASTIndexDeclaration : public IAST
{
public:
    static const auto DEFAULT_INDEX_GRANULARITY = 1uz;
    static const auto DEFAULT_ANNOY_INDEX_GRANULARITY = 100'000'000uz;
    static const auto DEFAULT_USEARCH_INDEX_GRANULARITY = 100'000'000uz;

    String name;
    IAST * expr;
    ASTFunction * type;
    UInt64 granularity;
    bool part_of_create_index_query = false;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Index"; }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&expr));
        f(reinterpret_cast<void **>(&type));
    }
};

}
