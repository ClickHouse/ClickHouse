#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MergeTree/MergeTreeProjections.h>

#include <numeric>

#include <boost/algorithm/string.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

void MergeTreeProjectionFactory::registerCreator(ProjectionDescription::Type projection_type, Creator creator)
{
    if (!creators.emplace(projection_type, std::move(creator)).second)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "MergeTreeProjectionFactory: the Projection creator name '{}' is not unique",
            ProjectionDescription::typeToString(projection_type));
}

MergeTreeProjectionPtr MergeTreeProjectionFactory::get(const ProjectionDescription & projection) const
{
    auto it = creators.find(projection.type);
    if (it == creators.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Projection type {} is not registered",
                        ProjectionDescription::typeToString(projection.type));

    return it->second(projection);
}


MergeTreeProjections MergeTreeProjectionFactory::getMany(const std::vector<ProjectionDescription> & projections) const
{
    MergeTreeProjections result;
    for (const auto & projection : projections)
        result.emplace_back(get(projection));
    return result;
}

void MergeTreeProjectionFactory::validate(const ProjectionDescription & projection) const
{
    if (startsWith(projection.name, "tmp_"))
        throw Exception("Projection's name cannot start with 'tmp_'", ErrorCodes::INCORRECT_QUERY);

    get(projection);
}

MergeTreeProjectionPtr normalProjectionCreator(const ProjectionDescription & projection)
{
    return std::make_shared<MergeTreeProjectionNormal>(projection);
}

MergeTreeProjectionPtr aggregateProjectionCreator(const ProjectionDescription & projection)
{
    return std::make_shared<MergeTreeProjectionAggregate>(projection);
}

MergeTreeProjectionFactory::MergeTreeProjectionFactory()
{
    registerCreator(ProjectionDescription::Type::Normal, normalProjectionCreator);
    registerCreator(ProjectionDescription::Type::Aggregate, aggregateProjectionCreator);
}

MergeTreeProjectionFactory & MergeTreeProjectionFactory::instance()
{
    static MergeTreeProjectionFactory instance;
    return instance;
}

}
