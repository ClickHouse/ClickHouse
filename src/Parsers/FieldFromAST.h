#pragma once
#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct FieldFromASTImpl : public CustomType::CustomTypeImpl
{
    static constexpr auto name = "AST";

    explicit FieldFromASTImpl(ASTPtr ast_) : ast(ast_) {}

    const char * getTypeName() const override { return name; }
    String toString(bool show_secrets) const override;
    bool isSecret() const override;

    [[noreturn]] void throwNotImplemented(std::string_view method) const;

    bool operator < (const CustomTypeImpl &) const override { throwNotImplemented("<"); }
    bool operator <= (const CustomTypeImpl &) const override { throwNotImplemented("<="); }
    bool operator > (const CustomTypeImpl &) const override { throwNotImplemented(">"); }
    bool operator >= (const CustomTypeImpl &) const override { throwNotImplemented(">="); }
    bool operator == (const CustomTypeImpl &) const override { throwNotImplemented("=="); }

    ASTPtr ast;
};

Field createFieldFromAST(ASTPtr ast);

}
