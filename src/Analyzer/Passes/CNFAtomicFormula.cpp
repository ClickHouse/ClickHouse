#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Passes/CNFAtomicFormula.h>

namespace DB::Analyzer
{

bool CNFAtomicFormula::operator==(const CNFAtomicFormula & rhs) const
{
    return negative == rhs.negative && node_with_hash == rhs.node_with_hash;
}

bool CNFAtomicFormula::operator<(const CNFAtomicFormula & rhs) const
{
    if (node_with_hash.hash > rhs.node_with_hash.hash)
        return false;

    return node_with_hash.hash < rhs.node_with_hash.hash || negative < rhs.negative;
}

}
