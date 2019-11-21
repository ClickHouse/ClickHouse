#pragma once

/**
  * This file implements some functions that are dependent on Field type.
  * Unlinke SettingsCommon.h, we only have to include it once for each
  * instantiation of SettingsCollection<>. This allows to work on Field without
  * always recompiling the entire project.
  */

#include <Common/SettingsChanges.h>

namespace DB
{

template <class Derived>
Field SettingsCollection<Derived>::const_reference::getValue() const
{
    return member->get_field(*collection);
}

template <class Derived>
void SettingsCollection<Derived>::reference::setValue(const Field & value)
{
    this->member->set_field(*const_cast<Derived *>(this->collection), value);
}

template <class Derived>
String SettingsCollection<Derived>::valueToString(size_t index, const Field & value)
{
    return members()[index].value_to_string(value);
}

template <class Derived>
String SettingsCollection<Derived>::valueToString(const StringRef & name, const Field & value)
{
    return members().findStrict(name)->value_to_string(value);
}

template <class Derived>
Field SettingsCollection<Derived>::valueToCorrespondingType(size_t index, const Field & value)
{
    return members()[index].value_to_corresponding_type(value);
}

template <class Derived>
Field SettingsCollection<Derived>::valueToCorrespondingType(const StringRef & name, const Field & value)
{
    return members().findStrict(name)->value_to_corresponding_type(value);
}

template <class Derived>
void SettingsCollection<Derived>::set(size_t index, const Field & value)
{
    (*this)[index].setValue(value);
}

template <class Derived>
void SettingsCollection<Derived>::set(const StringRef & name, const Field & value)
{
    (*this)[name].setValue(value);
}

template <class Derived>
Field SettingsCollection<Derived>::get(size_t index) const
{
    return (*this)[index].getValue();
}

template <class Derived>
Field SettingsCollection<Derived>::get(const StringRef & name) const
{
    return (*this)[name].getValue();
}

template <class Derived>
bool SettingsCollection<Derived>::tryGet(const StringRef & name, Field & value) const
{
    auto it = find(name);
    if (it == end())
        return false;
    value = it->getValue();
    return true;
}

template <class Derived>
bool SettingsCollection<Derived>::tryGet(const StringRef & name, String & value) const
{
    auto it = find(name);
    if (it == end())
        return false;
    value = it->getValueAsString();
    return true;
}

template <class Derived>
bool SettingsCollection<Derived>::operator ==(const Derived & rhs) const
{
    for (const auto & member : members())
    {
        bool left_changed = member.is_changed(castToDerived());
        bool right_changed = member.is_changed(rhs);
        if (left_changed || right_changed)
        {
            if (left_changed != right_changed)
                return false;
            if (member.get_field(castToDerived()) != member.get_field(rhs))
                return false;
        }
    }
    return true;
}

/// Gathers all changed values (e.g. for applying them later to another collection of settings).
template <class Derived>
SettingsChanges SettingsCollection<Derived>::changes() const
{
    SettingsChanges found_changes;
    for (const auto & member : members())
    {
        if (member.is_changed(castToDerived()))
            found_changes.push_back({member.name.toString(), member.get_field(castToDerived())});
    }
    return found_changes;
}

/// Applies change to concrete setting.
template <class Derived>
void SettingsCollection<Derived>::applyChange(const SettingChange & change)
{
    set(change.name, change.value);
}

/// Applies changes to the settings.
template <class Derived>
void SettingsCollection<Derived>::applyChanges(const SettingsChanges & changes)
{
    for (const SettingChange & change : changes)
        applyChange(change);
}

template <class Derived>
void SettingsCollection<Derived>::copyChangesFrom(const Derived & src)
{
    for (const auto & member : members())
        if (member.is_changed(src))
            member.set_field(castToDerived(), member.get_field(src));
}

template <class Derived>
void SettingsCollection<Derived>::copyChangesTo(Derived & dest) const
{
    dest.copyChangesFrom(castToDerived());
}

template <class Derived>
const typename SettingsCollection<Derived>::MemberInfos &
SettingsCollection<Derived>::members()
{
    return MemberInfos::instance();
}

} /* namespace DB */
