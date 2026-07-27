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
    static constexpr auto DEFAULT_INDEX_GRANULARITY = 1uz;
    static constexpr auto DEFAULT_TEXT_INDEX_GRANULARITY = 64uz;
    static constexpr auto DEFAULT_VECTOR_SIMILARITY_INDEX_GRANULARITY = 100'000'000uz;

    ASTIndexDeclaration(ASTPtr expression, ASTPtr type, const String & name_);

    String name;
    UInt64 granularity;
    bool part_of_create_index_query = false;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Index"; }

    ASTPtr clone() const override;

    ASTPtr getExpression() const;
    std::shared_ptr<ASTFunction> getType() const;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

private:
    static constexpr size_t expression_idx = 0;
    static constexpr size_t type_idx = 1;
};

}
