#pragma once

#include <optional>

#include <Parsers/IAST_fwd.h>
#include <Server/HTTP/HTMLForm.h>

namespace DB
{

struct HTTPQueryAST
{
    std::vector<ASTPtr> select_expressions;
    std::vector<ASTPtr> where_expressions;
    std::vector<ASTPtr> order_expressions;
};

template <typename T>
HTTPQueryAST getHTTPQueryAST(const T & params);

}
