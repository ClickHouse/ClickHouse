#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
/// Condition on the projection.
class IMergeTreeProjectionCondition
{
public:
    virtual ~IMergeTreeProjectionCondition() = default;
    /// Checks if this projection is useful for query.
    virtual bool canHandleQuery() const = 0;
};

using MergeTreeProjectionConditionPtr = std::shared_ptr<IMergeTreeProjectionCondition>;

struct IMergeTreeProjection
{
    IMergeTreeProjection(const ProjectionDescription & projection_) : projection(projection_) { }

    virtual ~IMergeTreeProjection() = default;

    /// gets directory name
    String getDirectoryName() const { return projection.name + ".proj"; }

    const String & getName() const { return projection.name; }

    Names getColumnsRequiredForProjectionCalc() const { return projection.required_columns; }

    const ProjectionDescription & projection;
};

using MergeTreeProjectionPtr = std::shared_ptr<const IMergeTreeProjection>;
using MergeTreeProjections = std::vector<MergeTreeProjectionPtr>;

class MergeTreeProjectionNormal : public IMergeTreeProjection
{
public:
    explicit MergeTreeProjectionNormal(const ProjectionDescription & projection_) : IMergeTreeProjection(projection_) { }

    ~MergeTreeProjectionNormal() override = default;
};

class MergeTreeProjectionAggregate : public IMergeTreeProjection
{
public:
    explicit MergeTreeProjectionAggregate(const ProjectionDescription & projection_) : IMergeTreeProjection(projection_) { }

    ~MergeTreeProjectionAggregate() override = default;
};

class MergeTreeProjectionFactory : private boost::noncopyable
{
public:
    static MergeTreeProjectionFactory & instance();

    using Creator = std::function<MergeTreeProjectionPtr(const ProjectionDescription & projection)>;

    void validate(const ProjectionDescription & projection) const;

    MergeTreeProjectionPtr get(const ProjectionDescription & projection) const;

    MergeTreeProjections getMany(const std::vector<ProjectionDescription> & projections) const;

    void registerCreator(ProjectionDescription::Type projection_type, Creator creator);

protected:
    MergeTreeProjectionFactory();

private:
    using Creators = std::unordered_map<ProjectionDescription::Type, Creator>;
    Creators creators;
};

}
