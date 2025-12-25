#pragma once

namespace DB
{

/** Generic CNF atomic formula that works with any node type that has hash support.
  *
  * Template parameter NodeWithHashType should have:
  * - A 'node' member that points to the actual node
  * - A 'hash' member for quick comparison
  * - operator== and operator< implemented
  *
  * This is a shared template used by both QueryTree CNF and ActionsDAG CNF implementations.
  */
template <typename NodeWithHashType>
struct CNFAtomicFormula
{
    bool negative = false;
    NodeWithHashType node_with_hash;

    bool operator==(const CNFAtomicFormula & rhs) const
    {
        return negative == rhs.negative && node_with_hash == rhs.node_with_hash;
    }

    bool operator<(const CNFAtomicFormula & rhs) const
    {
        if (node_with_hash.hash > rhs.node_with_hash.hash)
            return false;

        return node_with_hash.hash < rhs.node_with_hash.hash || negative < rhs.negative;
    }
};

}
