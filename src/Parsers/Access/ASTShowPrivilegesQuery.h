#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

struct ASTShowPrivilegesIDAndQueryName
{
    static constexpr auto ID = "ShowPrivilegesQuery";
    static constexpr auto Query = "SHOW PRIVILEGES";
    static constexpr auto Kind = IAST::QueryKind::Show;
};

using ASTShowPrivilegesQuery = ASTQueryWithOutputImpl<ASTShowPrivilegesIDAndQueryName>;

}
