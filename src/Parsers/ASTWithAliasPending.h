#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTWithAlias.h>


namespace DB
{

/** parametrised alias, e.g. select number as {custom_alias:Identifier} from system.numbers */
class ASTWithAliasPending : public ASTWithAlias
{
public:
    ASTPtr wrapped_ast;
    ASTPtr query_parameter;
    ASTWithAliasPending(ASTPtr, ASTPtr);
    ASTPtr clone() const override;
    String getID(char) const override { return "ASTWithAliasPending"; }
protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;

};

}
