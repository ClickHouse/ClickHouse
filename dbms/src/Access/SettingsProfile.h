#pragma once

#include <Access/IAttributes.h>
#include <Access/SettingsConstraints.h>
#include <Common/SettingsChanges.h>
#include <unordered_set>


namespace DB
{
/// Represents a settings profile in Role-based Access Control.
/// Syntax:
/// CREATE ROLE [IF NOT EXISTS] name
///
/// DROP ROLE [IF EXISTS] name
struct SettingsProfile : public IAttributes
{
    SettingsChanges settings;
    SettingsConstraints settings_constraints;

    std::unordered_set<UUID> apply_to_roles;
    bool apply_to_all_roles = false;
    std::unordered_set<UUID> except_roles;

    static const Type TYPE;
    const Type & getType() const override { return TYPE; }
    std::shared_ptr<IAttributes> clone() const override { return cloneImpl<SettingsProfile>(); }
    bool equal(const IAttributes & other) const override;
    bool hasReferences(const UUID & id) const override;
    void removeReferences(const UUID & id) override;
};
}
