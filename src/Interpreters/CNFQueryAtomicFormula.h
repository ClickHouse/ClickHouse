#pragma once

#include <memory>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

struct CNFQueryAtomicFormula
{
    bool negative = false;
    ASTPtr ast;

    /// for set
    bool operator<(const CNFQueryAtomicFormula & rhs) const;
    bool operator==(const CNFQueryAtomicFormula & rhs) const;
};

}
