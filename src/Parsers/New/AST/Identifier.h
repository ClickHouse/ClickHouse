#pragma once

#include <Parsers/New/AST/INode.h>

#include <string>


namespace DB::AST
{

class Identifier : public INode
{
    public:
        explicit Identifier(const std::string & name_);

    private:
        const std::string name;
};

class DatabaseIdentifier : public Identifier
{

};

class TableIdentifier : public Identifier
{
    public:
        TableIdentifier(PtrTo<DatabaseIdentifier> database, PtrTo<Identifier> name);

    private:
        PtrTo<DatabaseIdentifier> db;
};

class ColumnIdentifier : public Identifier
{

};

}
