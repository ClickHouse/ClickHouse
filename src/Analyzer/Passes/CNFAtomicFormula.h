#pragma once

#include <Analyzer/HashUtils.h>

namespace DB::Analyzer
{

struct CNFAtomicFormula
{
    bool negative = false;
    QueryTreeNodePtrWithHash node_with_hash;

    bool operator==(const CNFAtomicFormula & rhs) const;
    bool operator<(const CNFAtomicFormula & rhs) const;
};

}
