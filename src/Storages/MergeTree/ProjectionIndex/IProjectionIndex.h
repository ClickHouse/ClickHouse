#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <Common/PODArray_fwd.h>

namespace DB
{

class ColumnsDescription;

struct ProjectionDescription;

using IColumnPermutation = PaddedPODArray<size_t>;

class ASTProjectionDeclaration;

struct IndexDescription;

struct IMergeTreeIndex;
using MergeTreeIndexPtr = std::shared_ptr<const IMergeTreeIndex>;

struct MergeTreeSettings;

/// Base interface for projection index implementations.
class IProjectionIndex
{
public:
    virtual ~IProjectionIndex();

    virtual String getName() const = 0;

    virtual void fillProjectionDescription(
        ProjectionDescription & result, const IAST * index_expr, const ColumnsDescription & columns, ContextPtr query_context) const
        = 0;

    virtual Block calculate(
        const ProjectionDescription & projection_desc,
        const Block & block,
        UInt64 starting_offset,
        ContextPtr context,
        const IColumnPermutation * perm_ptr) const
        = 0;

    virtual std::shared_ptr<MergeTreeSettings> getDefaultSettings() const;

    virtual const IndexDescription * getIndexDescription() const { return nullptr; }

    virtual MergeTreeIndexPtr getIndex() const { return nullptr; }

    /// Returns the maximum number of rows supported by this index.
    /// Some indices are limited to 32-bit row counts (approx. 4.29 billion).
    /// Defaults to the maximum possible value if no specific limit exists.
    virtual UInt64 getMaxRows() const { return std::numeric_limits<UInt64>::max(); }
};

using ProjectionIndexPtr = std::shared_ptr<IProjectionIndex>;

class ProjectionIndexFactory : private boost::noncopyable
{
public:
    static ProjectionIndexFactory & instance();

    using Creator = std::function<ProjectionIndexPtr(const ASTProjectionDeclaration & proj)>;

    ProjectionIndexPtr get(const ASTProjectionDeclaration & proj) const;

    template <typename ProjectionIndex>
    void registerProjectionIndex()
    {
        creators.emplace(ProjectionIndex::name, ProjectionIndex::create);
    }

protected:
    ProjectionIndexFactory();

private:
    using Creators = std::unordered_map<std::string, Creator>;
    Creators creators;
};

}
