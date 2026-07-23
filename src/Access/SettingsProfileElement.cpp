#include <Access/SettingsProfileElement.h>
#include <Access/SettingsConstraints.h>
#include <Access/AccessControl.h>
#include <Access/SettingsProfile.h>
#include <Core/Settings.h>
#include <Common/SettingConstraintWritability.h>
#include <Common/SettingsChanges.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <base/removeDuplicates.h>
#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


SettingsProfileElement::SettingsProfileElement(const ASTSettingsProfileElement & ast)
{
    init(ast, nullptr);
}

SettingsProfileElement::SettingsProfileElement(const ASTSettingsProfileElement & ast, const AccessControl & access_control)
{
    init(ast, &access_control);
}

void SettingsProfileElement::init(const ASTSettingsProfileElement & ast, const AccessControl * access_control)
{
    auto name_to_id = [id_mode{ast.id_mode}, access_control](const String & name_) -> UUID
    {
        if (id_mode)
            return parse<UUID>(name_);
        assert(access_control);
        return access_control->getID<SettingsProfile>(name_);  /// NOLINT(clang-analyzer-core.CallAndMessage)
    };

    if (!ast.parent_profile.empty())
        parent_profile = name_to_id(ast.parent_profile);

    if (!ast.setting_name.empty())
    {
        setting_name = ast.setting_name;

        if (access_control)
        {
            /// Check if a setting with that name is allowed.
            if (!SettingsProfileElements::isAllowBackupSetting(setting_name))
                access_control->checkSettingNameIsAllowed(setting_name);
            /// Check if a CHANGEABLE_IN_READONLY is allowed.
            if (ast.writability == SettingConstraintWritability::CHANGEABLE_IN_READONLY && !access_control->doesSettingsConstraintsReplacePrevious())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "CHANGEABLE_IN_READONLY for {} "
                                "is not allowed unless settings_constraints_replace_previous is enabled", setting_name);
        }

        value = ast.value;
        min_value = ast.min_value;
        max_value = ast.max_value;
        writability = ast.writability;
        disallowed_values = ast.disallowed_values;

        if (value)
            value = Settings::castValueUtil(setting_name, *value);
        if (min_value)
            min_value = Settings::castValueUtil(setting_name, *min_value);
        if (max_value)
            max_value = Settings::castValueUtil(setting_name, *max_value);
        for (auto & allowed_value : disallowed_values)
            value = Settings::castValueUtil(setting_name, allowed_value);
    }
}

bool SettingsProfileElement::isConstraint() const
{
    return this->writability || this->min_value || this->max_value || !this->disallowed_values.empty();
}

std::shared_ptr<ASTSettingsProfileElement> SettingsProfileElement::toAST() const
{
    auto ast = std::make_shared<ASTSettingsProfileElement>();
    ast->id_mode = true;

    if (parent_profile)
        ast->parent_profile = ::DB::toString(*parent_profile);

    ast->setting_name = setting_name;
    ast->value = value;
    ast->min_value = min_value;
    ast->max_value = max_value;
    ast->disallowed_values = disallowed_values;
    ast->writability = writability;

    return ast;
}


std::shared_ptr<ASTSettingsProfileElement> SettingsProfileElement::toASTWithNames(const AccessControl & access_control) const
{
    auto ast = std::make_shared<ASTSettingsProfileElement>();

    if (parent_profile)
    {
        auto parent_profile_name = access_control.tryReadName(*parent_profile);
        if (parent_profile_name)
            ast->parent_profile = *parent_profile_name;
    }

    ast->setting_name = setting_name;
    ast->value = value;
    ast->min_value = min_value;
    ast->max_value = max_value;
    ast->disallowed_values = disallowed_values;
    ast->writability = writability;

    return ast;
}


SettingsProfileElements::SettingsProfileElements(const ASTSettingsProfileElements & ast, bool normalize_)
{
    for (const auto & ast_element : ast.elements)
        emplace_back(*ast_element);
    if (normalize_)
        normalize();
}

SettingsProfileElements::SettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControl & access_control, bool normalize_)
{
    for (const auto & ast_element : ast.elements)
        emplace_back(*ast_element, access_control);
    if (normalize_)
        normalize();
}


std::shared_ptr<ASTSettingsProfileElements> SettingsProfileElements::toAST() const
{
    auto res = std::make_shared<ASTSettingsProfileElements>();
    for (const auto & element : *this)
    {
        auto element_ast = element.toAST();
        if (!element_ast->empty())
            res->elements.push_back(element_ast);
    }
    return res;
}

std::shared_ptr<ASTSettingsProfileElements> SettingsProfileElements::toASTWithNames(const AccessControl & access_control) const
{
    auto res = std::make_shared<ASTSettingsProfileElements>();
    for (const auto & element : *this)
    {
        auto element_ast = element.toASTWithNames(access_control);
        if (!element_ast->empty())
            res->elements.push_back(element_ast);
    }
    return res;
}


std::vector<UUID> SettingsProfileElements::findDependencies() const
{
    std::vector<UUID> res;
    for (const auto & element : *this)
    {
        if (element.parent_profile)
            res.push_back(*element.parent_profile);
    }
    return res;
}


bool SettingsProfileElements::hasDependencies(const std::unordered_set<UUID> & ids) const
{
    std::vector<UUID> res;
    for (const auto & element : *this)
    {
        if (element.parent_profile && ids.contains(*element.parent_profile))
            return true;
    }
    return false;
}


void SettingsProfileElements::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    for (auto & element : *this)
    {
        if (element.parent_profile)
        {
            auto id = *element.parent_profile;
            auto it_new_id = old_to_new_ids.find(id);
            if (it_new_id != old_to_new_ids.end())
            {
                auto new_id = it_new_id->second;
                element.parent_profile = new_id;
            }
        }
    }
}


void SettingsProfileElements::copyDependenciesFrom(const SettingsProfileElements & src, const std::unordered_set<UUID> & ids)
{
    SettingsProfileElements new_elements;
    for (const auto & element : src)
    {
        if (element.parent_profile && ids.contains(*element.parent_profile))
        {
            SettingsProfileElement new_element;
            new_element.parent_profile = *element.parent_profile;
            new_elements.emplace_back(new_element);
        }
    }
    insert(begin(), new_elements.begin(), new_elements.end());
}


void SettingsProfileElements::removeDependencies(const std::unordered_set<UUID> & ids)
{
    std::erase_if(
        *this, [&](const SettingsProfileElement & element) { return element.parent_profile && ids.contains(*element.parent_profile); });
}


void SettingsProfileElements::removeSettingsKeepProfiles()
{
    for (auto & element : *this)
        element.setting_name.clear();

    std::erase_if(*this, [&](const SettingsProfileElement & element) { return element.setting_name.empty() && !element.parent_profile; });
}


void SettingsProfileElements::merge(const SettingsProfileElements & other, bool normalize_)
{
    insert(end(), other.begin(), other.end());
    if (normalize_)
        normalize();
}


Settings SettingsProfileElements::toSettings() const
{
    Settings res;
    for (const auto & elem : *this)
    {
        if (!elem.setting_name.empty() && !isAllowBackupSetting(elem.setting_name) && elem.value)
            res.set(elem.setting_name, *elem.value);
    }
    return res;
}

SettingsChanges SettingsProfileElements::toSettingsChanges() const
{
    SettingsChanges res;
    for (const auto & elem : *this)
    {
        if (!elem.setting_name.empty() && !isAllowBackupSetting(elem.setting_name))
        {
            if (elem.value)
                res.push_back({elem.setting_name, *elem.value});
        }
    }
    return res;
}

SettingsConstraints SettingsProfileElements::toSettingsConstraints(const AccessControl & access_control) const
{
    SettingsConstraints res{access_control};
    for (const auto & elem : *this)
        if (!elem.setting_name.empty() && elem.isConstraint() && !isAllowBackupSetting(elem.setting_name))
            res.set(
                elem.setting_name,
                elem.min_value ? *elem.min_value : Field{},
                elem.max_value ? *elem.max_value : Field{},
                elem.disallowed_values,
                elem.writability ? *elem.writability : SettingConstraintWritability::WRITABLE);
    return res;
}

std::vector<UUID> SettingsProfileElements::toProfileIDs() const
{
    std::vector<UUID> res;
    for (const auto & elem : *this)
    {
        if (elem.parent_profile)
            res.push_back(*elem.parent_profile);
    }

    /// If some profile occurs multiple times (with some other settings in between),
    /// the latest occurrence overrides all the previous ones.
    removeDuplicatesKeepLast(res);

    return res;
}


void SettingsProfileElements::normalize()
{
    /// Ensure that each element represents either a setting or a profile.
    {
        SettingsProfileElements new_elements;
        for (auto & element : *this)
        {
            if (element.parent_profile && !element.setting_name.empty())
            {
                SettingsProfileElement new_element;
                new_element.parent_profile = element.parent_profile;
                element.parent_profile.reset();
                new_elements.push_back(std::move(new_element));
            }
        }
        insert(end(), new_elements.begin(), new_elements.end());
    }

    /// Partitioning: first profiles, then settings.
    /// We use std::stable_partition() here because we want to preserve the relative order of profiles and the relative order of settings.
    /// (We need that order to be preserved to remove duplicates correctly - see below.)
    auto profiles_begin = begin();
    auto profiles_end = std::stable_partition(begin(), end(), [](const SettingsProfileElement & element) { return static_cast<bool>(element.parent_profile); });
    auto settings_begin = profiles_end;
    auto settings_end = end();

    /// Remove duplicates among profiles.
    /// We keep the last position of any used profile.
    /// It's important to keep exactly the last position (and not just any position) because profiles can override settings from each other.
    /// For example, [pr_A, pr_B, pr_A, pr_C] is always the same as [pr_B, pr_A, pr_C], but can be not the same as [pr_A, pr_B, pr_C]
    /// if pr_A and pr_B give different values to same settings.
    {
        boost::container::flat_set<UUID> profile_ids;
        profile_ids.reserve(profiles_end - profiles_begin);
        auto it = profiles_end;
        while (it != profiles_begin)
        {
            --it;
            auto & element = *it;
            if (element.parent_profile && !profile_ids.emplace(*element.parent_profile).second)
                element.parent_profile.reset();
        }
    }

    /// Remove duplicates among settings.
    /// We keep the first position of any used setting, and merge settings with the same name to that first element.
    {
        boost::container::flat_map<std::string_view, SettingsProfileElements::iterator> setting_name_to_first_encounter;
        setting_name_to_first_encounter.reserve(settings_end - settings_begin);
        for (auto it = settings_begin; it != settings_end; ++it)
        {
            auto & element = *it;
            auto first = setting_name_to_first_encounter.emplace(element.setting_name, it).first->second;
            if (it != first)
            {
                auto & first_element = *first;
                if (element.value)
                    first_element.value = element.value;
                if (element.min_value)
                    first_element.min_value = element.min_value;
                if (element.max_value)
                    first_element.max_value = element.max_value;
                if (!element.disallowed_values.empty())
                    first_element.disallowed_values = element.disallowed_values;
                if (element.writability)
                    first_element.writability = element.writability;
                element.setting_name.clear();
            }
        }
    }

    /// Remove empty elements.
    std::erase_if(*this, [](const SettingsProfileElement & element) { return element.empty(); });
}


bool SettingsProfileElements::isBackupAllowed() const
{
    for (const auto & setting : *this)
    {
        if (isAllowBackupSetting(setting.setting_name) && setting.value)
            return static_cast<bool>(SettingFieldBool{*setting.value});
    }
    return true;
}

bool SettingsProfileElements::isAllowBackupSetting(const String & setting_name)
{
    static constexpr std::string_view ALLOW_BACKUP_SETTING_NAME = "allow_backup";
    return Settings::resolveName(setting_name) == ALLOW_BACKUP_SETTING_NAME;
}


AlterSettingsProfileElements::AlterSettingsProfileElements(const SettingsProfileElements & ast)
{
    drop_all_settings = true;
    drop_all_profiles = true;
    add_settings = ast;
}

AlterSettingsProfileElements::AlterSettingsProfileElements(const ASTSettingsProfileElements & ast)
    : AlterSettingsProfileElements(SettingsProfileElements{ast})
{
}

AlterSettingsProfileElements::AlterSettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControl & access_control)
    : AlterSettingsProfileElements(SettingsProfileElements{ast, access_control})
{
}

AlterSettingsProfileElements::AlterSettingsProfileElements(const ASTAlterSettingsProfileElements & ast)
{
    drop_all_settings = ast.drop_all_settings;
    drop_all_profiles = ast.drop_all_profiles;

    if (ast.add_settings)
        add_settings = SettingsProfileElements{*ast.add_settings, /* normalize= */ false}; /// For "ALTER" the normalization is unnecessary.

    if (ast.modify_settings)
        modify_settings = SettingsProfileElements{*ast.modify_settings, /* normalize= */ false};

    if (ast.drop_settings)
        drop_settings = SettingsProfileElements{*ast.drop_settings, /* normalize= */ false};
}

AlterSettingsProfileElements::AlterSettingsProfileElements(const ASTAlterSettingsProfileElements & ast, const AccessControl & access_control)
{
    drop_all_settings = ast.drop_all_settings;
    drop_all_profiles = ast.drop_all_profiles;

    if (ast.add_settings)
        add_settings = SettingsProfileElements{*ast.add_settings, access_control, /* normalize= */ false}; /// For "ALTER" the normalization is unnecessary.

    if (ast.modify_settings)
        modify_settings = SettingsProfileElements{*ast.modify_settings, access_control, /* normalize= */ false};

    if (ast.drop_settings)
        drop_settings = SettingsProfileElements{*ast.drop_settings, access_control, /* normalize= */ false};
}

void SettingsProfileElements::applyChanges(const AlterSettingsProfileElements & changes)
{
    /// Apply "DROP" changes.
    if (changes.drop_all_profiles)
    {
        for (auto & element : *this)
            element.parent_profile.reset(); /// We only make this element empty, the element will be removed in normalizeProfileElements().
    }

    if (changes.drop_all_settings)
    {
        for (auto & element : *this)
            element.setting_name.clear(); /// We only make this element empty, the element will be removed in normalizeProfileElements().
    }

    auto apply_drop_setting = [&](const String & setting_name)
    {
        for (auto & element : *this)
        {
            if (element.setting_name == setting_name)
                element.setting_name.clear();
        }
    };

    auto apply_drop_profile = [&](const UUID & profile_id)
    {
        for (auto & element : *this)
        {
            if (element.parent_profile == profile_id)
                element.parent_profile.reset();
        }
    };

    for (const auto & drop : changes.drop_settings)
    {
        if (drop.parent_profile)
            apply_drop_profile(*drop.parent_profile);
        if (!drop.setting_name.empty())
            apply_drop_setting(drop.setting_name);
    }

    auto apply_modify_setting = [&](const SettingsProfileElement & modify)
    {
        SettingsProfileElement new_element;
        new_element.setting_name = modify.setting_name;
        new_element.value = modify.value;
        new_element.min_value = modify.min_value;
        new_element.max_value = modify.max_value;
        new_element.disallowed_values = modify.disallowed_values;
        new_element.writability = modify.writability;
        push_back(new_element); /// normalizeProfileElements() will merge this new element with the previous elements.
    };

    /// Apply "ADD" changes.
    auto apply_add_setting = [&](const SettingsProfileElement & add)
    {
        /// "ADD SETTING" must replace the value and the constraints of a setting, so first we need drop the previous elements for that setting.
        apply_drop_setting(add.setting_name);
        apply_modify_setting(add);
    };

    auto apply_add_profile = [&](const UUID & profile_id)
    {
        SettingsProfileElement new_element;
        new_element.parent_profile = profile_id;
        push_back(new_element); /// We don't care about possible duplicates here, normalizeProfileElements() will remove duplicates.
    };

    for (const auto & add : changes.add_settings)
    {
        if (add.parent_profile)
            apply_add_profile(*add.parent_profile);
        if (!add.setting_name.empty())
            apply_add_setting(add);
    }

    /// Apply "MODIFY" changes.
    for (const auto & modify : changes.modify_settings)
    {
        chassert(!modify.parent_profile); /// There is no such thing as "MODIFY PROFILE".
        if (!modify.setting_name.empty())
            apply_modify_setting(modify);
    }

    /// Remove empty elements and duplicates, and sort the result.
    normalize();
}

}
