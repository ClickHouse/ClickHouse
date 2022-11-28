#pragma once

#include <Parsers/IAST.h>
#include <DataTypes/IDataType.h>
#include <memory>
#include <unordered_map>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{

struct PreparedSetKey
{
    /// Prepared sets for tuple literals are indexed by the hash of the tree contents and by the desired
    /// data types of set elements (two different Sets can be required for two tuples with the same contents
    /// if left hand sides of the IN operators have different types).
    static PreparedSetKey forLiteral(const IAST & ast, DataTypes types_)
    {
        /// Remove LowCardinality types from type list because Set doesn't support LowCardinality keys now,
        ///   just converts LowCardinality to ordinary types.
        for (auto & type : types_)
            type = recursiveRemoveLowCardinality(type);

        PreparedSetKey key;
        key.ast_hash = ast.getTreeHash();
        key.types = std::move(types_);
        return key;
    }

    /// Prepared sets for subqueries are indexed only by the AST contents because the type of the resulting
    /// set is fully determined by the subquery.
    static PreparedSetKey forSubquery(const IAST & ast)
    {
        PreparedSetKey key;
        key.ast_hash = ast.getTreeHash();
        return key;
    }

    IAST::Hash ast_hash;
    DataTypes types; /// Empty for subqueries.

    bool operator==(const PreparedSetKey & other) const
    {
        if (ast_hash != other.ast_hash)
            return false;

        if (types.size() != other.types.size())
            return false;

        for (size_t i = 0; i < types.size(); ++i)
        {
            if (!types[i]->equals(*other.types[i]))
                return false;
        }

        return true;
    }

    struct Hash
    {
        UInt64 operator()(const PreparedSetKey & key) const { return key.ast_hash.first; }
    };
};

class Set;
using SetPtr = std::shared_ptr<Set>;

using PreparedSets = std::unordered_map<PreparedSetKey, SetPtr, PreparedSetKey::Hash>;

}
