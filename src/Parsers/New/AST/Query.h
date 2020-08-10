#pragma once

#include <Parsers/New/AST/INode.h>

#include <Parsers/ASTQueryWithOutput.h>


namespace DB::AST
{

class Query : public INode {
    public:
        void setOutFile(PtrTo<StringLiteral> literal);
        void setFormat(PtrTo<Identifier> id);

    protected:
        void convertToOldPartially(const std::shared_ptr<ASTQueryWithOutput> & query) const;

    private:
        // TODO: put them to |children|
        PtrTo<StringLiteral> out_file;
        PtrTo<Identifier> format;
};

}
