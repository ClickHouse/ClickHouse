#pragma once
#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/formatAST.h>

namespace DB
{

struct FieldFromASTImpl : public CustomType::CustomTypeImpl
{
    static constexpr auto name = "AST";

    explicit FieldFromASTImpl(ASTPtr ast_) : ast(ast_) {}

    const char * getTypeName() const override { return name; }
    String toString() const override { return serializeAST(*ast); }

    ASTPtr ast;
};

Field createFieldFromAST(ASTPtr ast)
{
    return CustomType(std::make_shared<FieldFromASTImpl>(ast));
}

}
