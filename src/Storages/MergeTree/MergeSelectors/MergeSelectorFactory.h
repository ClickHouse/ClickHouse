#pragma once

#include <unordered_map>
#include <functional>
#include <memory>
#include <string>
#include <any>
#include <boost/noncopyable.hpp>

#include <Core/MergeSelectorAlgorithm.h>

namespace DB
{

class IMergeSelector;

using MergeSelectorPtr = std::shared_ptr<IMergeSelector>;

class MergeSelectorFactory final : private boost::noncopyable
{
private:
    using Creator = std::function<MergeSelectorPtr(std::any)>;
    using CreatorByNameMap = std::unordered_map<std::string, Creator>;
    using EnumToName = std::unordered_map<MergeSelectorAlgorithm, std::string>;

    CreatorByNameMap creators;
    EnumToName enum_to_name_mapping;
    MergeSelectorFactory() = default;
public:
    static MergeSelectorFactory & instance();

    MergeSelectorPtr get(const std::string & name, const std::any & settings = {}) const;
    MergeSelectorPtr get(MergeSelectorAlgorithm algorithm, const std::any & settings = {}) const;

    void registerPrivateSelector(std::string name, Creator && creator);
    void registerPublicSelector(std::string name, MergeSelectorAlgorithm enum_value, Creator && creator);
};

}
