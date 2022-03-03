#include <Access/RolesOrUsersSet.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/formatAST.h>
#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <base/sort.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


RolesOrUsersSet::RolesOrUsersSet() = default;
RolesOrUsersSet::RolesOrUsersSet(const RolesOrUsersSet & src) = default;
RolesOrUsersSet & RolesOrUsersSet::operator =(const RolesOrUsersSet & src) = default;
RolesOrUsersSet::RolesOrUsersSet(RolesOrUsersSet && src) noexcept = default;
RolesOrUsersSet & RolesOrUsersSet::operator =(RolesOrUsersSet && src) noexcept = default;


RolesOrUsersSet::RolesOrUsersSet(AllTag)
{
    all = true;
}

RolesOrUsersSet::RolesOrUsersSet(const UUID & id)
{
    add(id);
}


RolesOrUsersSet::RolesOrUsersSet(const std::vector<UUID> & ids_)
{
    add(ids_);
}


RolesOrUsersSet::RolesOrUsersSet(const ASTRolesOrUsersSet & ast)
{
    init(ast, nullptr);
}

RolesOrUsersSet::RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const std::optional<UUID> & current_user_id)
{
    init(ast, nullptr, current_user_id);
}

RolesOrUsersSet::RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const AccessControl & access_control)
{
    init(ast, &access_control);
}

RolesOrUsersSet::RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const AccessControl & access_control, const std::optional<UUID> & current_user_id)
{
    init(ast, &access_control, current_user_id);
}

void RolesOrUsersSet::init(const ASTRolesOrUsersSet & ast, const AccessControl * access_control, const std::optional<UUID> & current_user_id)
{
    all = ast.all;

    auto name_to_id = [&ast, access_control](const String & name) -> UUID
    {
        if (ast.id_mode)
            return parse<UUID>(name);
        assert(access_control);
        if (ast.allow_users && ast.allow_roles)
        {
            auto id = access_control->find<User>(name);
            if (id)
                return *id;
            return access_control->getID<Role>(name);
        }
        else if (ast.allow_users)
        {
            return access_control->getID<User>(name);
        }
        else
        {
            assert(ast.allow_roles);
            return access_control->getID<Role>(name);
        }
    };

    if (!ast.names.empty() && !all)
    {
        ids.reserve(ast.names.size());
        for (const String & name : ast.names)
            ids.insert(name_to_id(name));
    }

    if (ast.current_user && !all)
    {
        assert(current_user_id);
        ids.insert(*current_user_id);
    }

    if (!ast.except_names.empty())
    {
        except_ids.reserve(ast.except_names.size());
        for (const String & name : ast.except_names)
            except_ids.insert(name_to_id(name));
    }

    if (ast.except_current_user)
    {
        assert(current_user_id);
        except_ids.insert(*current_user_id);
    }

    for (const UUID & id : except_ids)
        ids.erase(id);
}


std::shared_ptr<ASTRolesOrUsersSet> RolesOrUsersSet::toAST() const
{
    auto ast = std::make_shared<ASTRolesOrUsersSet>();
    ast->id_mode = true;
    ast->all = all;

    if (!ids.empty() && !all)
    {
        ast->names.reserve(ids.size());
        for (const UUID & id : ids)
            ast->names.emplace_back(::DB::toString(id));
        ::sort(ast->names.begin(), ast->names.end());
    }

    if (!except_ids.empty())
    {
        ast->except_names.reserve(except_ids.size());
        for (const UUID & except_id : except_ids)
            ast->except_names.emplace_back(::DB::toString(except_id));
        ::sort(ast->except_names.begin(), ast->except_names.end());
    }

    return ast;
}


std::shared_ptr<ASTRolesOrUsersSet> RolesOrUsersSet::toASTWithNames(const AccessControl & access_control) const
{
    auto ast = std::make_shared<ASTRolesOrUsersSet>();
    ast->all = all;

    if (!ids.empty() && !all)
    {
        ast->names.reserve(ids.size());
        for (const UUID & id : ids)
        {
            auto name = access_control.tryReadName(id);
            if (name)
                ast->names.emplace_back(std::move(*name));
        }
        ::sort(ast->names.begin(), ast->names.end());
    }

    if (!except_ids.empty())
    {
        ast->except_names.reserve(except_ids.size());
        for (const UUID & except_id : except_ids)
        {
            auto except_name = access_control.tryReadName(except_id);
            if (except_name)
                ast->except_names.emplace_back(std::move(*except_name));
        }
        ::sort(ast->except_names.begin(), ast->except_names.end());
    }

    return ast;
}


String RolesOrUsersSet::toString() const
{
    auto ast = toAST();
    return serializeAST(*ast);
}


String RolesOrUsersSet::toStringWithNames(const AccessControl & access_control) const
{
    auto ast = toASTWithNames(access_control);
    return serializeAST(*ast);
}


bool RolesOrUsersSet::empty() const
{
    return ids.empty() && !all;
}


void RolesOrUsersSet::clear()
{
    ids.clear();
    all = false;
    except_ids.clear();
}


void RolesOrUsersSet::add(const UUID & id)
{
    if (!all)
        ids.insert(id);
    except_ids.erase(id);
}


void RolesOrUsersSet::add(const std::vector<UUID> & ids_)
{
    if (!all)
        ids.insert(ids_.begin(), ids_.end());
    for (const auto & id : ids_)
        except_ids.erase(id);
}


bool RolesOrUsersSet::match(const UUID & id) const
{
    return (all || ids.count(id)) && !except_ids.count(id);
}


bool RolesOrUsersSet::match(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles) const
{
    if (!all && !ids.count(user_id))
    {
        bool found_enabled_role = std::any_of(
            enabled_roles.begin(), enabled_roles.end(), [this](const UUID & enabled_role) { return ids.count(enabled_role); });
        if (!found_enabled_role)
            return false;
    }

    if (except_ids.count(user_id))
        return false;

    bool in_except_list = std::any_of(
        enabled_roles.begin(), enabled_roles.end(), [this](const UUID & enabled_role) { return except_ids.count(enabled_role); });
    return !in_except_list;
}


std::vector<UUID> RolesOrUsersSet::getMatchingIDs() const
{
    if (all)
        throw Exception("getAllMatchingIDs() can't get ALL ids without access_control", ErrorCodes::LOGICAL_ERROR);
    std::vector<UUID> res;
    boost::range::set_difference(ids, except_ids, std::back_inserter(res));
    return res;
}


std::vector<UUID> RolesOrUsersSet::getMatchingIDs(const AccessControl & access_control) const
{
    if (!all)
        return getMatchingIDs();

    std::vector<UUID> res;
    for (const UUID & id : access_control.findAll<User>())
    {
        if (match(id))
            res.push_back(id);
    }
    for (const UUID & id : access_control.findAll<Role>())
    {
        if (match(id))
            res.push_back(id);
    }

    return res;
}


bool operator ==(const RolesOrUsersSet & lhs, const RolesOrUsersSet & rhs)
{
    return (lhs.all == rhs.all) && (lhs.ids == rhs.ids) && (lhs.except_ids == rhs.except_ids);
}

}
