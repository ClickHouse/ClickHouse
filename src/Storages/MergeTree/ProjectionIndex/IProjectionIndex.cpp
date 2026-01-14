#include <Storages/MergeTree/ProjectionIndex/IProjectionIndex.h>

#include <numeric>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexBasic.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexText.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

std::shared_ptr<MergeTreeSettings> IProjectionIndex::getDefaultSettings() const
{
    return std::make_shared<MergeTreeSettings>();
}

IProjectionIndex::~IProjectionIndex() = default;

ProjectionIndexPtr ProjectionIndexFactory::get(const ASTProjectionDeclaration & proj) const
{
    auto it = creators.find(proj.type->name);
    if (it == creators.end())
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Unknown projection index type '{}'. Available projection index types: {}",
            proj.type->name,
            std::accumulate(
                creators.cbegin(),
                creators.cend(),
                std::string{},
                [](auto && left, const auto & right) -> std::string
                {
                    if (left.empty())
                        return right.first;
                    return left + ", " + right.first;
                }));
    }

    return it->second(proj);
}

ProjectionIndexFactory::ProjectionIndexFactory()
{
    registerProjectionIndex<ProjectionIndexBasic>();
    registerProjectionIndex<ProjectionIndexText>();
}

ProjectionIndexFactory & ProjectionIndexFactory::instance()
{
    static ProjectionIndexFactory instance;
    return instance;
}

}
