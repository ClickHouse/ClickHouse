#include <Access/RolesOrUsersSet.h>
#include <Access/AccessControlManager.h>
#include <Access/VisibleAccessEntities.h>
#include <Access/ContextAccess.h>
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
    extern const int UNKNOWN_ROLE;
}

namespace
{
    void throwRoleOrUserNotFound(const String & name)
    {
        throw Exception("There is no user or role " + backQuote(name) + " in user directories", ErrorCodes::UNKNOWN_ROLE);
    }


    std::shared_ptr<ASTRolesOrUsersSet> toASTWithNamesImpl(const RolesOrUsersSet & set, const AccessControlManager & manager, const VisibleAccessEntities * visible_entities)
    {
        auto ast = std::make_shared<ASTRolesOrUsersSet>();
        ast->all = set.all;

        auto id_to_name = [&](const UUID & id) -> std::optional<String>
        {
            if (visible_entities && !visible_entities->isVisible(id))
                return {};
            return manager.tryReadName(id);
        };

        if (!set.ids.empty())
        {
            ast->names.reserve(set.ids.size());
            for (const UUID & id : set.ids)
            {
                auto name = id_to_name(id);
                if (name)
                    ast->names.emplace_back(std::move(*name));
            }
            boost::range::sort(ast->names);
        }

        if (!set.except_ids.empty())
        {
            ast->except_names.reserve(set.except_ids.size());
            for (const UUID & except_id : set.except_ids)
            {
                auto except_name = id_to_name(except_id);
                if (except_name)
                    ast->except_names.emplace_back(std::move(*except_name));
            }
            boost::range::sort(ast->except_names);
        }

        return ast;
    }


    Strings ASTToStrings(ASTRolesOrUsersSet && ast)
    {
        Strings res;
        size_t count = (ast.all ? 1 : ast.names.size()) + (ast.except_names.empty() ? 0 : (1 + ast.except_names.size()));
        res.reserve(count);

        if (ast.all)
            res.emplace_back("ALL");
        else
            res.insert(res.end(), std::make_move_iterator(ast.names.begin()), std::make_move_iterator(ast.names.end()));

        if (!ast.except_names.empty())
        {
            res.emplace_back("EXCEPT");
            res.insert(res.end(), std::make_move_iterator(ast.except_names.begin()), std::make_move_iterator(ast.except_names.end()));
        }

        return res;
    }


    std::vector<UUID> getMatchingIDsImpl(const RolesOrUsersSet & set, const AccessControlManager & manager, const VisibleAccessEntities * visible_entities)
    {
        std::vector<UUID> res;
        if (!set.all)
        {
            boost::range::set_difference(set.ids, set.except_ids, std::back_inserter(res));
        }
        else
        {
            if (visible_entities)
            {
                res = visible_entities->findAll<User>();
                boost::range::push_back(res, visible_entities->findAll<Role>());
            }
            else
            {
                res = manager.findAll<User>();
                boost::range::push_back(res, manager.findAll<Role>());
            }

            boost::range::remove_erase_if(
                res,
                [&](const UUID & id) -> bool
                {
                    return !set.match(id);
                });
        }

        if (visible_entities)
        {
            boost::range::remove_erase_if(
                res,
                [&](const UUID & id) -> bool
                {
                    return !visible_entities->isVisible(id);
                });
        }

        return res;
    }
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
    init(ast, nullptr, nullptr, {});
}

RolesOrUsersSet::RolesOrUsersSet(
    const ASTRolesOrUsersSet & ast, const AccessControlManager & manager, const std::optional<UUID> & current_user_id)
{
    init(ast, &manager, nullptr, current_user_id);
}

RolesOrUsersSet::RolesOrUsersSet(const ASTRolesOrUsersSet & ast, const VisibleAccessEntities & visible_entities)
{
    init(ast, &visible_entities.getAccessControlManager(), &visible_entities, visible_entities.getAccess()->getUserID());
}

void RolesOrUsersSet::init(const ASTRolesOrUsersSet & ast, const AccessControlManager * manager, const VisibleAccessEntities * visible_entities, const std::optional<UUID> & current_user_id)
{
    all = ast.all;

    auto name_to_id = [&](const String & name) -> UUID
    {
        if (ast.id_mode)
            return parse<UUID>(name);
        assert(manager);
        if (ast.allow_user_names && ast.allow_role_names)
        {
            std::optional<UUID> id;
            if (visible_entities)
                id = visible_entities->find<User>(name);
            else
                id = manager->find<User>(name);
            if (id)
                return *id;
            if (visible_entities)
                id = visible_entities->find<Role>(name);
            else
                id = manager->find<Role>(name);
            if (id)
                return *id;
            throwRoleOrUserNotFound(name);
        }
        else if (ast.allow_user_names)
        {
            if (visible_entities)
                return visible_entities->getID<User>(name);
            else
                return manager->getID<User>(name);
        }
        else
        {
            assert(ast.allow_role_names);
            if (visible_entities)
                return visible_entities->getID<Role>(name);
            else
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
    return toASTWithNamesImpl(*this, manager, nullptr);
}

std::shared_ptr<ASTRolesOrUsersSet> RolesOrUsersSet::toASTWithNames(const VisibleAccessEntities & visible_entities) const
{
    return toASTWithNamesImpl(*this, visible_entities.getAccessControlManager(), &visible_entities);
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

String RolesOrUsersSet::toStringWithNames(const VisibleAccessEntities & visible_entities) const
{
    auto ast = toASTWithNames(visible_entities);
    return serializeAST(*ast);
}


Strings RolesOrUsersSet::toStringsWithNames(const AccessControlManager & manager) const
{
    auto ast = toASTWithNames(manager);
    return ASTToStrings(std::move(*ast));
}

Strings RolesOrUsersSet::toStringsWithNames(const VisibleAccessEntities & visible_entities) const
{
    auto ast = toASTWithNames(visible_entities);
    return ASTToStrings(std::move(*ast));
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
    return getMatchingIDsImpl(*this, manager, nullptr);
}

std::vector<UUID> RolesOrUsersSet::getMatchingIDs(const VisibleAccessEntities & visible_entities) const
{
    return getMatchingIDsImpl(*this, visible_entities.getAccessControlManager(), &visible_entities);
}


bool operator ==(const RolesOrUsersSet & lhs, const RolesOrUsersSet & rhs)
{
    return (lhs.all == rhs.all) && (lhs.ids == rhs.ids) && (lhs.except_ids == rhs.except_ids);
}

}
