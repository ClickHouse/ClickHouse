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

    /// The source text with the secrets of the embedded queries (`.ch.sql "..."`) hidden, or empty if it has none.
    /// The program is logged in this form when it cannot be compiled: see `parseQQuery`.
    String text_without_secrets;

    bool hasSecretParts() const override { return !text_without_secrets.empty(); }

    String getID(char /*delimiter*/) const override { return "QQuery"; }

    ASTPtr clone() const override { return make_intrusive<ASTQQuery>(*this); }

    QueryKind getQueryKind() const override { return QueryKind::Select; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        const String & shown = settings.show_secrets || text_without_secrets.empty() ? text : text_without_secrets;
        ostr.write(shown.data(), shown.size());
    }
};

}
