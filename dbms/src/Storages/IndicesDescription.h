#pragma once

#include <Core/Types.h>

#include <memory>
#include <vector>

namespace DB
{

class ASTIndexDeclaration;
using IndicesASTs = std::vector<std::shared_ptr<ASTIndexDeclaration>>;

struct IndicesDescription
{
    IndicesASTs indices;

    IndicesDescription() = default;

    bool empty() const;
    bool has(const String & name) const;

    String toString() const;
    static IndicesDescription parse(const String & str);
};

}
