#include <DataTypes/EnumValues.h>
#include <boost/algorithm/string.hpp>
#include <base/sort.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int EMPTY_DATA_PASSED;
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
}

template <typename T>
EnumValues<T>::EnumValues(const Values & values_)
    : values(values_)
{
    if (values.empty())
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "DataTypeEnum enumeration cannot be empty");

    ::sort(std::begin(values), std::end(values), [] (auto & left, auto & right)
    {
        return left.second < right.second;
    });

    fillMaps();
}

template <typename T>
void EnumValues<T>::fillMaps()
{
    for (const auto & name_and_value : values)
    {
        const auto inserted_value = name_to_value_map.insert(
            { StringRef{name_and_value.first}, name_and_value.second });

        if (!inserted_value.second)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate names in enum: '{}' = {} and {}",
                    name_and_value.first, toString(name_and_value.second), toString(inserted_value.first->getMapped()));

        const auto inserted_name = value_to_name_map.insert(
            { name_and_value.second, StringRef{name_and_value.first} });

        if (!inserted_name.second)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate values in enum: '{}' = {} and '{}'",
                    name_and_value.first, toString(name_and_value.second), toString((*inserted_name.first).first));
    }
}

template <typename T>
T EnumValues<T>::getValue(StringRef field_name) const
{
    T x;
    if (auto it = name_to_value_map.find(field_name); it != name_to_value_map.end())
    {
        return it->getMapped();
    }
    if (tryParse(x, field_name.data, field_name.size) && value_to_name_map.contains(x))
    {
        /// If we fail to find given string in enum names, we will try to treat it as enum id.
        return x;
    }

    auto hints = this->getHints(field_name.toString());
    auto hints_string = !hints.empty() ? ", maybe you meant: " + toString(hints) : "";
    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM, "Unknown element '{}' for enum{}", field_name.toString(), hints_string);
}

template <typename T>
bool EnumValues<T>::tryGetValue(T & x, StringRef field_name) const
{
    if (auto it = name_to_value_map.find(field_name); it != name_to_value_map.end())
    {
        x = it->getMapped();
        return true;
    }

    /// If we fail to find given string in enum names, we will try to treat it as enum id.
    return tryParse(x, field_name.data, field_name.size) && value_to_name_map.contains(x);
}

template <typename T>
Names EnumValues<T>::getAllRegisteredNames() const
{
    Names result;
    for (const auto & value : values)
        result.emplace_back(value.first);
    return result;
}

template <typename T>
std::unordered_set<String> EnumValues<T>::getSetOfAllNames(bool to_lower) const
{
    std::unordered_set<String> result;
    for (const auto & value : values)
        result.insert(to_lower ? boost::algorithm::to_lower_copy(value.first) : value.first);
    return result;
}

template <typename T>
std::unordered_set<T> EnumValues<T>::getSetOfAllValues() const
{
    std::unordered_set<T> result;
    for (const auto & value : values)
        result.insert(value.second);
    return result;
}

template class EnumValues<Int8>;
template class EnumValues<Int16>;

}
