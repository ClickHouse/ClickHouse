#pragma once

#include <IO/WriteBuffer.h>
#include <Parsers/IAST.h>
#include <Parsers/Q/QSyntax.h>


namespace DB
{

/// A program in the q language (the `q` dialect).
///
/// The parser checks only the syntax of the program. The program is compiled into a ClickHouse query
/// on the server (see `compileQProgram`), because the compilation depends on the structure of the tables.
class ASTQQuery : public IAST
{
public:
    /// The source text of the program.
    String text;
    std::shared_ptr<const QProgram> program;

    String getID(char /*delimiter*/) const override { return "QQuery"; }

    ASTPtr clone() const override { return make_intrusive<ASTQQuery>(*this); }

    QueryKind getQueryKind() const override { return QueryKind::Select; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        ostr.write(text.data(), text.size());
    }
};

}
