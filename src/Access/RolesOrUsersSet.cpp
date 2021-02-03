#include <Access/RolesOrUsersSet.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/formatAST.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


RolesOrUsersSet::RolesOrUsersSet() = default;
RolesOrUsersSet::RolesOrUsersSet(const RolesOrUsersSet & src) = default;
RolesOrUsersSet & RolesOrUsersSet::operator =(const RolesOrUsersSet & src) = default;
RolesOrUsersSet::RolesOrUsersSet(RolesOrUsersSet && src) = default;
RolesOrUsersSet & RolesOrUsersSet::operator =(RolesOrUsersSet && src) = default;


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

RolesOrUsersSet::RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const AccessControlManager & manager)
{
    init(ast, &manager);
}

RolesOrUsersSet::RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const AccessControlManager & manager, const std::optional<UUID> & current_user_id)
{
    init(ast, &manager, current_user_id);
}

void RolesOrUsersSet::init(const ASTRolesOrUsersSet & ast, const AccessControlManager * manager, const std::optional<UUID> & current_user_id)
{
    all = ast.all;

    auto name_to_id = [&ast, manager](const String & name) -> UUID
    {
        if (ast.id_mode)
            return parse<UUID>(name);
        assert(manager);
        if (ast.allow_user_names && ast.allow_role_names)
        {
            auto id = manager->find<User>(name);
            if (id)
                return *id;
            return manager->getID<Role>(name);
        }
        else if (ast.allow_user_names)
        {
            return manager->getID<User>(name);
        }
        else
        {
            assert(ast.allow_role_names);
            return manager->getID<Role>(name);
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
        for (const String & except_name : ast.except_names)
            except_ids.insert(name_to_id(except_name));
    }

    if (ast.except_current_user)
    {
        assert(current_user_id);
        except_ids.insert(*current_user_id);
    }

    for (const UUID & except_id : except_ids)
        ids.erase(except_id);
}


std::shared_ptr<ASTRolesOrUsersSet> RolesOrUsersSet::toAST() const
{
    auto ast = std::make_shared<ASTRolesOrUsersSet>();
    ast->id_mode = true;
    ast->all = all;

    if (!ids.empty())
    {
        ast->names.reserve(ids.size());
        for (const UUID & id : ids)
            ast->names.emplace_back(::DB::toString(id));
        boost::range::sort(ast->names);
    }

    if (!except_ids.empty())
    {
        ast->except_names.reserve(except_ids.size());
        for (const UUID & except_id : except_ids)
            ast->except_names.emplace_back(::DB::toString(except_id));
        boost::range::sort(ast->except_names);
    }

    return ast;
}


std::shared_ptr<ASTRolesOrUsersSet> RolesOrUsersSet::toASTWithNames(const AccessControlManager & manager) const
{
    auto ast = std::make_shared<ASTRolesOrUsersSet>();
    ast->all = all;

    if (!ids.empty())
    {
        ast->names.reserve(ids.size());
        for (const UUID & id : ids)
        {
            auto name = manager.tryReadName(id);
            if (name)
                ast->names.emplace_back(std::move(*name));
        }
        boost::range::sort(ast->names);
    }

    if (!except_ids.empty())
    {
        ast->except_names.reserve(except_ids.size());
        for (const UUID & except_id : except_ids)
        {
            auto except_name = manager.tryReadName(except_id);
            if (except_name)
                ast->except_names.emplace_back(std::move(*except_name));
        }
        boost::range::sort(ast->except_names);
    }

    return ast;
}


String RolesOrUsersSet::toString() const
{
    auto ast = toAST();
    return serializeAST(*ast);
}


String RolesOrUsersSet::toStringWithNames(const AccessControlManager & manager) const
{
    auto ast = toASTWithNames(manager);
    return serializeAST(*ast);
}


Strings RolesOrUsersSet::toStringsWithNames(const AccessControlManager & manager) const
{
    if (!all && ids.empty())
        return {};

    Strings res;
    res.reserve(ids.size() + except_ids.size());

    if (all)
        res.emplace_back("ALL");
    else
    {
        for (const UUID & id : ids)
        {
            auto name = manager.tryReadName(id);
            if (name)
                res.emplace_back(std::move(*name));
        }
        std::sort(res.begin(), res.end());
    }

    if (!except_ids.empty())
    {
        res.emplace_back("EXCEPT");
        size_t old_size = res.size();
        for (const UUID & id : except_ids)
        {
            auto name = manager.tryReadName(id);
            if (name)
                res.emplace_back(std::move(*name));
        }
        std::sort(res.begin() + old_size, res.end());
    }

    return res;
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
    ids.insert(id);
}


void RolesOrUsersSet::add(const std::vector<UUID> & ids_)
{
    for (const auto & id : ids_)
        add(id);
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
        throw Exception("getAllMatchingIDs() can't get ALL ids without manager", ErrorCodes::LOGICAL_ERROR);
    std::vector<UUID> res;
    boost::range::set_difference(ids, except_ids, std::back_inserter(res));
    return res;
}


std::vector<UUID> RolesOrUsersSet::getMatchingIDs(const AccessControlManager & manager) const
{
    if (!all)
        return getMatchingIDs();

    std::vector<UUID> res;
    for (const UUID & id : manager.findAll<User>())
    {
        if (match(id))
            res.push_back(id);
    }
    for (const UUID & id : manager.findAll<Role>())
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
