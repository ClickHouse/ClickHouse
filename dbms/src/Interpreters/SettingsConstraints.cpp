#include <limits>
#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <common/StringRef.h>
#include <Interpreters/Settings.h>
#include <Interpreters/SettingsConstraints.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_SETTING;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SETTINGS_CONSTRAINTS_VIOLATION;
}


class SettingsConstraints::Constraint
{
public:
    const char * const name;
    Constraint(const char * name_) : name(name_) {}
    virtual ~Constraint() = default;
    virtual void setMaxValue(const String &)
    {
        throw Exception("Setting max value for " + String(name) + " isn't supported", ErrorCodes::NOT_IMPLEMENTED);
    }
    virtual void check(const Field &) const {}
};


template <typename SettingType>
class SettingsConstraints::ConstraintImpl : public Constraint
{
    using Constraint::Constraint;
};


template <typename T>
class SettingsConstraints::ConstraintImpl<SettingNumber<T>> : public Constraint
{
public:
    using Constraint::Constraint;

    void setMaxValue(const String & max_value_) override
    {
        max_value = parse<T>(max_value_);
    }

    void check(const Field & field) const override
    {
        T value;
        if (field.getType() == Field::Types::String)
            value = parse<T>(field.get<String>());
        else
            value = applyVisitor(FieldVisitorConvertToNumber<T>(), field);
        checkValue(value);
    }

protected:
    void checkValue(T value) const
    {
        if (value > max_value)
            throw Exception(
                "Setting " + String(name) + " violates constraints: " + toString(value)
                    + " greater than the maximum allowed value " + toString(max_value),
                ErrorCodes::SETTINGS_CONSTRAINTS_VIOLATION);
    }

    T max_value = std::numeric_limits<T>::max();
};


template struct SettingsConstraints::ConstraintImpl<SettingUInt64>;
template struct SettingsConstraints::ConstraintImpl<SettingInt64>;
template struct SettingsConstraints::ConstraintImpl<SettingFloat>;
template struct SettingsConstraints::ConstraintImpl<SettingMaxThreads>;
template struct SettingsConstraints::ConstraintImpl<SettingSeconds>;
template struct SettingsConstraints::ConstraintImpl<SettingMilliseconds>;
template struct SettingsConstraints::ConstraintImpl<SettingLoadBalancing>;
template struct SettingsConstraints::ConstraintImpl<SettingJoinStrictness>;
template struct SettingsConstraints::ConstraintImpl<SettingTotalsMode>;
template struct SettingsConstraints::ConstraintImpl<SettingOverflowMode<false>>;
template struct SettingsConstraints::ConstraintImpl<SettingOverflowMode<true>>;
template struct SettingsConstraints::ConstraintImpl<SettingDistributedProductMode>;
template struct SettingsConstraints::ConstraintImpl<SettingString>;
template struct SettingsConstraints::ConstraintImpl<SettingChar>;
template struct SettingsConstraints::ConstraintImpl<SettingDateTimeInputFormat>;
template struct SettingsConstraints::ConstraintImpl<SettingLogsLevel>;


SettingsConstraints::SettingsConstraints() = default;
SettingsConstraints::~SettingsConstraints() = default;


void SettingsConstraints::check(const String & name, const String & value) const
{
    check(name, Field(value));
}

void SettingsConstraints::check(const String & name, const Field & field) const
{
    const auto * constraint = findConstraint(name);
    if (constraint)
        constraint->check(field);
}

template<typename SettingType>
void SettingsConstraints::check(const String & name, const SettingType & setting) const
{
    check(name, setting.toString());
}


template void SettingsConstraints::check(const String & name, const SettingUInt64 & setting) const;
template void SettingsConstraints::check(const String & name, const SettingInt64 & setting) const;
template void SettingsConstraints::check(const String & name, const SettingFloat & setting) const;
template void SettingsConstraints::check(const String & name, const SettingMaxThreads & setting) const;
template void SettingsConstraints::check(const String & name, const SettingSeconds & setting) const;
template void SettingsConstraints::check(const String & name, const SettingMilliseconds & setting) const;
template void SettingsConstraints::check(const String & name, const SettingLoadBalancing & setting) const;
template void SettingsConstraints::check(const String & name, const SettingJoinStrictness & setting) const;
template void SettingsConstraints::check(const String & name, const SettingTotalsMode & setting) const;
template void SettingsConstraints::check(const String & name, const SettingOverflowMode<false> & setting) const;
template void SettingsConstraints::check(const String & name, const SettingOverflowMode<true> & setting) const;
template void SettingsConstraints::check(const String & name, const SettingDistributedProductMode & setting) const;
template void SettingsConstraints::check(const String & name, const SettingString & setting) const;
template void SettingsConstraints::check(const String & name, const SettingChar & setting) const;
template void SettingsConstraints::check(const String & name, const SettingDateTimeInputFormat & setting) const;
template void SettingsConstraints::check(const String & name, const SettingLogsLevel & setting) const;


SettingsConstraints::Constraint * SettingsConstraints::findConstraint(const String & name)
{
    auto it = name_to_constraint_map.find(name);
    if (it == name_to_constraint_map.end())
        return nullptr;
    return it->second.get();
}

const SettingsConstraints::Constraint * SettingsConstraints::findConstraint(const String & name) const
{
    auto it = name_to_constraint_map.find(name);
    for (const auto & x : name_to_constraint_map)
        std::cout << "name_to_constraint_map: " << x.first << ", searching for " << name << std::endl;
    if (it == name_to_constraint_map.end())
        return nullptr;
    return it->second.get();
}

SettingsConstraints::Constraint & SettingsConstraints::findOrCreateConstraint(const String & name)
{
    auto it = name_to_constraint_map.find(name);
    if (it == name_to_constraint_map.end())
    {
        static const std::unordered_map<StringRef, std::function<std::unique_ptr<Constraint>()>> creator_map = []
        {
            std::unordered_map<StringRef, std::function<std::unique_ptr<Constraint>()>> creator_map;

#define ADD_ENTRY_TO_CREATOR_MAP(TYPE, NAME, DEFAULT, DESCRIPTION) \
            creator_map.emplace( \
                StringRef(#NAME, strlen(#NAME)), \
                std::function<std::unique_ptr<Constraint>()>( \
                    []() -> std::unique_ptr<Constraint> \
                    { \
                        return std::make_unique<ConstraintImpl<TYPE>>(#NAME); \
                    }));

            APPLY_FOR_SETTINGS(ADD_ENTRY_TO_CREATOR_MAP)

#undef ADD_ENTRY_TO_CREATOR_MAP
            return creator_map;
        }();
        auto creator_it = creator_map.find(name);
        if (creator_it == creator_map.end())
            throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
        auto new_constraint = (creator_it->second)();
        it = name_to_constraint_map.emplace(name, std::move(new_constraint)).first;
    }
    return *it->second;
}


void SettingsConstraints::setMaxValue(const String & name, const String & max_value)
{
    findOrCreateConstraint(name).setMaxValue(max_value);
}


void SettingsConstraints::setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config)
{
    String parent_profile = "profiles." + profile_name + ".profile";
    if (config.has(parent_profile))
        setProfile(parent_profile, config); // Inheritance of one profile from another.

    String path_to_constraints = "profiles." + profile_name + ".constraints";
    if (config.has(path_to_constraints))
        loadFromConfig(path_to_constraints, config);
}


void SettingsConstraints::loadFromConfig(const String & path_to_constraints, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(path_to_constraints))
        throw Exception("There is no path '" + path_to_constraints + "' in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    Poco::Util::AbstractConfiguration::Keys names;
    config.keys(path_to_constraints, names);

    for (const String & name : names)
    {
        String path_to_name = path_to_constraints + "." + name;
        Poco::Util::AbstractConfiguration::Keys constraint_types;
        config.keys(path_to_name, constraint_types);
        for (const String & constraint_type : constraint_types)
        {
            String path_to_type = path_to_name + "." + constraint_type;
            if (constraint_type == "max")
                setMaxValue(name, config.getString(path_to_type));
            else
                throw Exception("Setting " + constraint_type + " value for " + name + " isn't supported", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
}

}
