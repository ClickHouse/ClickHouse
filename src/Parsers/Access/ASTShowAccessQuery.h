#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

struct ASTShowAccessQueryNames
{
    static constexpr auto ID = "ShowAccessQuery";
    static constexpr auto Query = "SHOW ACCESS";
    static constexpr auto Kind = IAST::QueryKind::Show;
};

using ASTShowAccessQuery = ASTQueryWithOutputImpl<ASTShowAccessQueryNames>;

}
