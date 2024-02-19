#include <Common/KnownObjectNames.h>
#include <Poco/String.h>


namespace DB
{

bool KnownObjectNames::exists(const String & name) const
{
    std::lock_guard lock{mutex};
    if (names.contains(name))
        return true;

    if (!case_insensitive_names.empty())
    {
        String lower_name = Poco::toLower(name);
        if (case_insensitive_names.contains(lower_name))
            return true;
    }

    return false;
}


void KnownObjectNames::add(const String & name, bool case_insensitive)
{
    std::lock_guard lock{mutex};
    if (case_insensitive)
        case_insensitive_names.emplace(Poco::toLower(name));
    else
        names.emplace(name);
}


KnownTableFunctionNames & KnownTableFunctionNames::instance()
{
    static KnownTableFunctionNames the_instance;
    return the_instance;
}


KnownFormatNames & KnownFormatNames::instance()
{
    static KnownFormatNames the_instance;
    return the_instance;
}


void KnownFormatNames::add(const String & name, bool case_insensitive)
{
    std::lock_guard lock{mutex};
    if (case_insensitive)
    {
        String lower_case_name = Poco::toLower(name);
        case_insensitive_names.emplace(lower_case_name);
        case_insensitive_names_map[lower_case_name] = name;
    }
    else
        names.emplace(name);
}


String KnownFormatNames::getOriginalFormatNameIfExists(const String & name) const
{
    std::lock_guard lock{mutex};
    String lower_case_name = Poco::toLower(name);
    auto it = case_insensitive_names_map.find(lower_case_name);
    if (case_insensitive_names_map.end() != it)
        return it->second;
    return name;
}

}
