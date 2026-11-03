#include <Storages/MergeTree/ProjectionIndex/IProjectionIndex.h>

#include <numeric>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexBasic.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
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
}

ProjectionIndexFactory & ProjectionIndexFactory::instance()
{
    static ProjectionIndexFactory instance;
    return instance;
}

}
