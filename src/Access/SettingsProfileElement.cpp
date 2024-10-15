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
        return access_control->getID<SettingsProfile>(name_);
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

        if (value)
            value = Settings::castValueUtil(setting_name, *value);
        if (min_value)
            min_value = Settings::castValueUtil(setting_name, *min_value);
        if (max_value)
            max_value = Settings::castValueUtil(setting_name, *max_value);
    }
}

bool SettingsProfileElement::isConstraint() const
{
    return this->writability || this->min_value || this->max_value;
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
    ast->writability = writability;

    return ast;
}


SettingsProfileElements::SettingsProfileElements(const ASTSettingsProfileElements & ast)
{
    for (const auto & ast_element : ast.elements)
        emplace_back(*ast_element);
}

SettingsProfileElements::SettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControl & access_control)
{
    for (const auto & ast_element : ast.elements)
        emplace_back(*ast_element, access_control);
}


std::shared_ptr<ASTSettingsProfileElements> SettingsProfileElements::toAST() const
{
    auto res = std::make_shared<ASTSettingsProfileElements>();
    for (const auto & element : *this)
        res->elements.push_back(element.toAST());
    return res;
}

std::shared_ptr<ASTSettingsProfileElements> SettingsProfileElements::toASTWithNames(const AccessControl & access_control) const
{
    auto res = std::make_shared<ASTSettingsProfileElements>();
    for (const auto & element : *this)
        res->elements.push_back(element.toASTWithNames(access_control));
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


void SettingsProfileElements::merge(const SettingsProfileElements & other)
{
    insert(end(), other.begin(), other.end());
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

}
