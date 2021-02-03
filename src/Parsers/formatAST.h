#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class WriteBuffer;

/** Takes a syntax tree and turns it back into text.
  * In case of INSERT query, the data will be missing.
  */
void formatAST(const IAST & ast, WriteBuffer & buf, bool hilite = true, bool one_line = false);

String serializeAST(const IAST & ast, bool one_line = true);

inline WriteBuffer & operator<<(WriteBuffer & buf, const IAST & ast)
{
    formatAST(ast, buf, false, true);
    return buf;
}

inline WriteBuffer & operator<<(WriteBuffer & buf, const ASTPtr & ast)
{
    formatAST(*ast, buf, false, true);
    return buf;
}

}

template<>
struct fmt::formatter<DB::ASTPtr>
{
    template<typename ParseContext>
    constexpr auto parse(ParseContext & context)
    {
        return context.begin();
    }

    template<typename FormatContext>
    auto format(const DB::ASTPtr & ast, FormatContext & context)
    {
        return fmt::format_to(context.out(), "{}", DB::serializeAST(*ast));
    }
};

