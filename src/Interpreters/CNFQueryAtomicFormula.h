#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

struct CNFQueryAtomicFormula
{
    bool negative = false;
    ASTPtr ast;

    /// for set
    bool operator<(const CNFQueryAtomicFormula & rhs) const;
    bool operator==(const CNFQueryAtomicFormula & rhs) const;
};

}
