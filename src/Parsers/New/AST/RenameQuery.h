#pragma once

#include <Parsers/New/AST/DDLQuery.h>


namespace DB::AST
{

class RenameQuery : public DDLQuery
{
    public:
        explicit RenameQuery(PtrTo<List<TableIdentifier>> list);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            EXPRS = 0,  // List<TableIdentifier>
        };
};

}
