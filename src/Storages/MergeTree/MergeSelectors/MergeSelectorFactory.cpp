#include <Storages/MergeTree/MergeSelectors/MergeSelectorFactory.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeSelectorFactory & MergeSelectorFactory::instance()
{
    static MergeSelectorFactory ret;
    return ret;
}

void MergeSelectorFactory::registerPrivateSelector(std::string name, MergeSelectorFactory::Creator && creator)
{
    if (!creators.emplace(name, creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge selector '{}' already exists", name);
}


void MergeSelectorFactory::registerPublicSelector(std::string name, MergeSelectorAlgorithm enum_value, Creator && creator)
{
    registerPrivateSelector(name, std::move(creator));
    if (!enum_to_name_mapping.emplace(enum_value, name).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge select with enum value {} already exists with different name", enum_value);
}

MergeSelectorPtr MergeSelectorFactory::get(const std::string & name, const std::any & settings) const
{
    auto it = creators.find(name);
    if (it == creators.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown merge selector {}", name);

    return it->second(settings);
}

MergeSelectorPtr MergeSelectorFactory::get(MergeSelectorAlgorithm algorithm, const std::any & settings) const
{
    auto it = enum_to_name_mapping.find(algorithm);
    if (it == enum_to_name_mapping.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown merge selector {}", algorithm);
    return get(it->second, settings);

}


}
