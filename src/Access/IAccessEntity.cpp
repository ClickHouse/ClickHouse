#include <Access/IAccessEntity.h>


namespace DB
{

bool IAccessEntity::equal(const IAccessEntity & other) const
{
    return (name == other.name) && (getType() == other.getType());
}

void IAccessEntity::replaceDependencies(std::shared_ptr<const IAccessEntity> & entity, const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    if (old_to_new_ids.empty())
        return;

    bool need_replace_dependencies = false;
    auto dependencies = entity->findDependencies();
    for (const auto & dependency : dependencies)
    {
        if (old_to_new_ids.contains(dependency))
        {
            need_replace_dependencies = true;
            break;
        }
    }

    if (!need_replace_dependencies)
        return;

    auto new_entity = entity->clone();
    new_entity->replaceDependencies(old_to_new_ids);
    entity = new_entity;
}

}
