#pragma once

#include <Parsers/ASTConstraintDeclaration.h>


namespace DB
{

using ConstraintsASTs = std::vector<std::shared_ptr<ASTConstraintDeclaration>>;

struct ConstraintsDescription
{
    ConstraintsASTs constraints;

    ConstraintsDescription() = default;

    bool empty() const { return constraints.empty(); }
    String toString() const;

    static ConstraintsDescription parse(const String & str);
};

}
