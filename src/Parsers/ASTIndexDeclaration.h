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
    String name;
    IAST * expr;
    ASTFunction * type;
    UInt64 granularity;
    bool part_of_create_index_query = false;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Index"; }

    ASTPtr clone() const override;
    void formatImpl(const FormattingBuffer & out) const override;
};

}
