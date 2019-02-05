#pragma once

#include <Parsers/ASTIndexDeclaration.h>


namespace DB
{

using IndicesAsts = std::vector<std::shared_ptr<ASTIndexDeclaration>>;

struct IndicesDescription
{
    IndicesAsts indices;

    IndicesDescription() = default;

    String toString() const;

    static IndicesDescription parse(const String & str);
};

}
