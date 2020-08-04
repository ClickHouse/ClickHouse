#pragma once

#include <Parsers/New/AST/INode.h>


namespace DB::AST
{

class Query : public INode {
public:
    void setOutFile(PtrTo<StringLiteral> literal);
    void setFormat(PtrTo<Identifier> id);

private:
    PtrTo<StringLiteral> out_file;
    PtrTo<Identifier> format;
};

}
