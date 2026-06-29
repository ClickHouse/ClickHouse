#include <Interpreters/CNFQueryAtomicFormula.h>
#include <Parsers/IAST.h>

namespace DB
{

/// for set
bool CNFQueryAtomicFormula::operator<(const CNFQueryAtomicFormula & rhs) const
{
    return ast->getTreeHash(/*ignore_aliases=*/ true) == rhs.ast->getTreeHash(/*ignore_aliases=*/ true)
        ? negative < rhs.negative
        : ast->getTreeHash(/*ignore_aliases=*/ true) < rhs.ast->getTreeHash(/*ignore_aliases=*/ true);
}

bool CNFQueryAtomicFormula::operator==(const CNFQueryAtomicFormula & rhs) const
{
    return negative == rhs.negative &&
        ast->getTreeHash(/*ignore_aliases=*/ true) == rhs.ast->getTreeHash(/*ignore_aliases=*/ true) &&
        ast->getColumnName() == rhs.ast->getColumnName();
}

}
