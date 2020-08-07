#pragma once

#include <Parsers/New/AST/INode.h>

#include <string>


namespace DB::AST
{

class Identifier : public INode
{
    public:
        explicit Identifier(const std::string & name_);

        const auto & getName() const { return name; }

        ASTPtr convertToOld() const override;

        virtual String getQualifiedName() const { return name; };

    private:
        const std::string name;

        String dumpInfo() const override { return getQualifiedName(); }
};

class DatabaseIdentifier : public Identifier
{
    public:
        explicit DatabaseIdentifier(PtrTo<Identifier> name);
};

class TableIdentifier : public Identifier
{
    public:
        TableIdentifier(PtrTo<DatabaseIdentifier> database, PtrTo<Identifier> name);

        String getQualifiedName() const override { return (db ? db->getQualifiedName() + "." : String()) + getName(); }

    private:
        PtrTo<DatabaseIdentifier> db;
};

class ColumnIdentifier : public Identifier
{
    public:
        ColumnIdentifier(PtrTo<TableIdentifier> table, PtrTo<Identifier> name);

        String getQualifiedName() const override { return (table ? table->getQualifiedName() + "." : String()) + getName(); }

    private:
        PtrTo<TableIdentifier> table;
};

}
