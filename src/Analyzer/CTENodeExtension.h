#pragma once

#include <optional>
#include <Analyzer/IQueryTreeNode.h>
#include <Parsers/IAST.h>
#include "Common/SipHash.h"
#include <IO/Operators.h>

namespace DB
{

/// CTE node extension for QueryNode and UnionNode
class CTENodeExtension
{
protected:
    bool is_cte = false;
    bool is_materialized_cte = false;
    std::shared_ptr<bool> cte_ref = std::make_shared<bool>();
    std::string cte_name;
    std::string unique_cte_name;
    ASTPtr materialized_cte_engine;
    static constexpr auto CTE_PREFIX = "__cte_";

public:
    /// Returns true if query node is CTE, false otherwise
    bool isCTE() const
    {
        return is_cte;
    }

    /// Set query node is CTE
    void setIsCTE(bool is_cte_value)
    {
        is_cte = is_cte_value;
    }

    /// Get query node CTE name
    const std::string & getCTEName() const
    {
        return cte_name;
    }

    /// Get query node unique CTE name
    const std::string & getUniqueCTEName() const
    {
        return unique_cte_name;
    }

    /// Set query node CTE name
    void setCTEName(std::string cte_name_value, size_t cte_index)
    {
        if (!cte_name_value.empty())
        {
            cte_name = std::move(cte_name_value);
            unique_cte_name = CTE_PREFIX + cte_name + "_" + std::to_string(cte_index);
        }
    }

    /// Returns true if the query node is a materialized CTE
    bool isMaterializedCTE() const
    {
        return is_materialized_cte;
    }

    /// Set query node is materialized CTE
    void setIsMaterializedCTE(bool is_materialized_cte_value)
    {
        is_materialized_cte = is_materialized_cte_value;
    }

    /// Set storage engine for the holder if query node is a materialized CTE
    void setMaterializedCTEEngine(ASTPtr materialized_cte_engine_value)
    {
        materialized_cte_engine = std::move(materialized_cte_engine_value);
    }

    /// Get storage engine for the holder
    ASTPtr getMaterializedCTEEngine() const
    {
        return materialized_cte_engine;
    }


    /// Get CTE reference counter
    size_t getCTEReferenceCounter() const
    {
        return cte_ref.use_count();
    }

    /// Clone internal state from another node
    void cteCloneFrom(const CTENodeExtension & src)
    {
        is_cte = src.is_cte;
        is_materialized_cte = src.is_materialized_cte;
        cte_name = src.cte_name;
        unique_cte_name = src.unique_cte_name;
        if (src.materialized_cte_engine)
            materialized_cte_engine = src.materialized_cte_engine->clone();
        cte_ref = src.cte_ref; /// Not cloning reference counter
    }

    /// Update hash
    void cteUpdateTreeHash(IQueryTreeNode::HashState & state) const
    {
        state.update(is_cte);
        state.update(is_materialized_cte);
        state.update(cte_name.size());
        state.update(cte_name);
        state.update(cte_name.size());
        state.update(unique_cte_name);
        if (materialized_cte_engine)
            materialized_cte_engine->updateTreeHashImpl(state, true);
    }

    bool cteIsEqual(const CTENodeExtension & rhs) const
    {
        if ((materialized_cte_engine && !rhs.materialized_cte_engine) || (!materialized_cte_engine && rhs.materialized_cte_engine))
            return false;

        return is_cte == rhs.is_cte
            && is_materialized_cte == rhs.is_materialized_cte
            && cte_name == rhs.cte_name
            && unique_cte_name == rhs.unique_cte_name
            && (!materialized_cte_engine || materialized_cte_engine->getTreeHash(true) == rhs.materialized_cte_engine->getTreeHash(true));
    }

    void cteDumpTree(WriteBuffer & buffer) const
    {
        if (is_cte)
            buffer << ", is_cte: " << is_cte;

        if (is_materialized_cte)
            buffer << ", is_materialized_cte: " << is_materialized_cte;

        if (materialized_cte_engine)
            buffer << ", materialized_cte_engine: " << materialized_cte_engine->formatForLogging();

        if (cte_ref.use_count() > 0)
            buffer << ", cte_ref: " << cte_ref.use_count();

        if (!cte_name.empty())
            buffer << ", cte_name: " << cte_name;

        if (!unique_cte_name.empty())
            buffer << ", unique_cte_name: " << unique_cte_name;
    }
};

}
