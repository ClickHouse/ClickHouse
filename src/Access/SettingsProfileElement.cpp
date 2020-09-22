#include <Access/SettingsProfileElement.h>
#include <Access/SettingsConstraints.h>
#include <Access/AccessControlManager.h>
#include <Access/SettingsProfile.h>
#include <Parsers/ASTSettingsProfileElement.h>
#include <Core/Settings.h>
#include <Common/SettingsChanges.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
SettingsProfileElement::SettingsProfileElement(const ASTSettingsProfileElement & ast)
{
    init(ast, nullptr);
}

SettingsProfileElement::SettingsProfileElement(const ASTSettingsProfileElement & ast, const AccessControlManager & manager)
{
    init(ast, &manager);
}

void SettingsProfileElement::init(const ASTSettingsProfileElement & ast, const AccessControlManager * manager)
{
    auto name_to_id = [id_mode{ast.id_mode}, manager](const String & name_) -> UUID
    {
        if (id_mode)
            return parse<UUID>(name_);
        assert(manager);
        return manager->getID<SettingsProfile>(name_);
    };

    if (!ast.parent_profile.empty())
        parent_profile = name_to_id(ast.parent_profile);

    if (!ast.setting_name.empty())
    {
        setting_name = ast.setting_name;

        /// Optionally check if a setting with that name is allowed.
        if (manager)
            manager->checkSettingNameIsAllowed(setting_name);

        value = ast.value;
        min_value = ast.min_value;
        max_value = ast.max_value;
        readonly = ast.readonly;

        if (!value.isNull())
            value = Settings::castValueUtil(setting_name, value);
        if (!min_value.isNull())
            min_value = Settings::castValueUtil(setting_name, min_value);
        if (!max_value.isNull())
            max_value = Settings::castValueUtil(setting_name, max_value);
    }
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
    ast->readonly = readonly;

    return ast;
}


std::shared_ptr<ASTSettingsProfileElement> SettingsProfileElement::toASTWithNames(const AccessControlManager & manager) const
{
    auto ast = std::make_shared<ASTSettingsProfileElement>();

    if (parent_profile)
    {
        auto parent_profile_name = manager.tryReadName(*parent_profile);
        if (parent_profile_name)
            ast->parent_profile = *parent_profile_name;
    }

    ast->setting_name = setting_name;
    ast->value = value;
    ast->min_value = min_value;
    ast->max_value = max_value;
    ast->readonly = readonly;

    return ast;
}


SettingsProfileElements::SettingsProfileElements(const ASTSettingsProfileElements & ast)
{
    for (const auto & ast_element : ast.elements)
        emplace_back(*ast_element);
}

SettingsProfileElements::SettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControlManager & manager)
{
    for (const auto & ast_element : ast.elements)
        emplace_back(*ast_element, manager);
}


std::shared_ptr<ASTSettingsProfileElements> SettingsProfileElements::toAST() const
{
    auto res = std::make_shared<ASTSettingsProfileElements>();
    for (const auto & element : *this)
        res->elements.push_back(element.toAST());
    return res;
}

std::shared_ptr<ASTSettingsProfileElements> SettingsProfileElements::toASTWithNames(const AccessControlManager & manager) const
{
    auto res = std::make_shared<ASTSettingsProfileElements>();
    for (const auto & element : *this)
        res->elements.push_back(element.toASTWithNames(manager));
    return res;
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
        if (!elem.setting_name.empty() && !elem.value.isNull())
            res.set(elem.setting_name, elem.value);
    }
    return res;
}

SettingsChanges SettingsProfileElements::toSettingsChanges() const
{
    SettingsChanges res;
    for (const auto & elem : *this)
    {
        if (!elem.setting_name.empty() && !elem.value.isNull())
            res.push_back({elem.setting_name, elem.value});
    }
    return res;
}

SettingsConstraints SettingsProfileElements::toSettingsConstraints(const AccessControlManager & manager) const
{
    SettingsConstraints res{manager};
    for (const auto & elem : *this)
    {
        if (!elem.setting_name.empty())
        {
            if (!elem.min_value.isNull())
                res.setMinValue(elem.setting_name, elem.min_value);
            if (!elem.max_value.isNull())
                res.setMaxValue(elem.setting_name, elem.max_value);
            if (elem.readonly)
                res.setReadOnly(elem.setting_name, *elem.readonly);
        }
    }
    return res;
}

}
