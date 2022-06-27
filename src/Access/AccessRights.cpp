#include <Access/AccessRights.h>
#include <Common/logger_useful.h>
#include <base/sort.h>
#include <boost/container/small_vector.hpp>
#include <boost/range/adaptor/map.hpp>
#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    struct ProtoElement
    {
        AccessFlags access_flags;
        boost::container::small_vector<std::string_view, 3> full_name;
        bool grant_option = false;
        bool is_partial_revoke = false;

        friend bool operator<(const ProtoElement & left, const ProtoElement & right)
        {
            /// Compare components alphabetically.
            size_t min_size = std::min(left.full_name.size(), right.full_name.size());
            for (size_t i = 0; i != min_size; ++i)
            {
                int cmp = left.full_name[i].compare(right.full_name[i]);
                if (cmp != 0)
                    return cmp < 0;
            }

            /// Names with less number of components first.
            if (left.full_name.size() != right.full_name.size())
                return left.full_name.size() < right.full_name.size();

            /// Grants before partial revokes.
            if (left.is_partial_revoke != right.is_partial_revoke)
                return right.is_partial_revoke; /// if left is grant, right is partial revoke, we assume left < right

            /// Grants with grant option after other grants.
            /// Revoke grant option after normal revokes.
            if (left.grant_option != right.grant_option)
                return right.grant_option; /// if left is without grant option, and right is with grant option, we assume left < right

            return (left.access_flags < right.access_flags);
        }

        AccessRightsElement getResult() const
        {
            AccessRightsElement res;
            res.access_flags = access_flags;
            res.grant_option = grant_option;
            res.is_partial_revoke = is_partial_revoke;
            switch (full_name.size())
            {
                case 0:
                {
                    res.any_database = true;
                    res.any_table = true;
                    res.any_column = true;
                    break;
                }
                case 1:
                {
                    res.any_database = false;
                    res.database = full_name[0];
                    res.any_table = true;
                    res.any_column = true;
                    break;
                }
                case 2:
                {
                    res.any_database = false;
                    res.database = full_name[0];
                    res.any_table = false;
                    res.table = full_name[1];
                    res.any_column = true;
                    break;
                }
                case 3:
                {
                    res.any_database = false;
                    res.database = full_name[0];
                    res.any_table = false;
                    res.table = full_name[1];
                    res.any_column = false;
                    res.columns.emplace_back(full_name[2]);
                    break;
                }
            }
            return res;
        }
    };

    class ProtoElements : public std::vector<ProtoElement>
    {
    public:
        AccessRightsElements getResult() const
        {
            ProtoElements sorted = *this;
            ::sort(sorted.begin(), sorted.end());
            AccessRightsElements res;
            res.reserve(sorted.size());

            for (size_t i = 0; i != sorted.size();)
            {
                size_t count_elements_with_diff_columns = sorted.countElementsWithDifferenceInColumnOnly(i);
                if (count_elements_with_diff_columns == 1)
                {
                    /// Easy case: one Element is converted to one AccessRightsElement.
                    const auto & element = sorted[i];
                    if (element.access_flags)
                        res.emplace_back(element.getResult());
                    ++i;
                }
                else
                {
                    /// Difficult case: multiple Elements are converted to one or multiple AccessRightsElements.
                    sorted.appendResultWithElementsWithDifferenceInColumnOnly(i, count_elements_with_diff_columns, res);
                    i += count_elements_with_diff_columns;
                }
            }
            return res;
        }

    private:
        size_t countElementsWithDifferenceInColumnOnly(size_t start) const
        {
            const auto & start_element = (*this)[start];
            if ((start_element.full_name.size() != 3) || (start == size() - 1))
                return 1;

            auto it = std::find_if(begin() + start + 1, end(), [&](const ProtoElement & element)
            {
                return (element.full_name.size() != 3) || (element.full_name[0] != start_element.full_name[0])
                    || (element.full_name[1] != start_element.full_name[1]) || (element.grant_option != start_element.grant_option)
                    || (element.is_partial_revoke != start_element.is_partial_revoke);
            });

            return it - (begin() + start);
        }

        /// Collects columns together to write multiple columns into one AccessRightsElement.
        /// That procedure allows to output access rights in more compact way,
        /// e.g. "SELECT(x, y)" instead of "SELECT(x), SELECT(y)".
        void appendResultWithElementsWithDifferenceInColumnOnly(size_t start, size_t count, AccessRightsElements & res) const
        {
            const auto * pbegin = data() + start;
            const auto * pend = pbegin + count;
            AccessFlags handled_flags;

            while (pbegin < pend)
            {
                while (pbegin < pend && !(pbegin->access_flags - handled_flags))
                    ++pbegin;

                while (pbegin < pend && !((pend - 1)->access_flags - handled_flags))
                    --pend;

                if (pbegin >= pend)
                    break;

                AccessFlags common_flags = (pbegin->access_flags - handled_flags);
                for (const auto * element = pbegin + 1; element != pend; ++element)
                {
                    if (auto new_common_flags = (element->access_flags - handled_flags) & common_flags)
                        common_flags = new_common_flags;
                }

                res.emplace_back();
                auto & back = res.back();
                back.grant_option = pbegin->grant_option;
                back.is_partial_revoke = pbegin->is_partial_revoke;
                back.any_database = false;
                back.database = pbegin->full_name[0];
                back.any_table = false;
                back.table = pbegin->full_name[1];
                back.any_column = false;
                back.access_flags = common_flags;
                for (const auto * element = pbegin; element != pend; ++element)
                {
                    if (((element->access_flags - handled_flags) & common_flags) == common_flags)
                        back.columns.emplace_back(element->full_name[2]);
                }

                handled_flags |= common_flags;
            }
        }
    };


    enum Level
    {
        GLOBAL_LEVEL,
        DATABASE_LEVEL,
        TABLE_LEVEL,
        COLUMN_LEVEL,
    };

    AccessFlags getAllGrantableFlags(Level level)
    {
        switch (level)
        {
            case GLOBAL_LEVEL: return AccessFlags::allFlagsGrantableOnGlobalLevel();
            case DATABASE_LEVEL: return AccessFlags::allFlagsGrantableOnDatabaseLevel();
            case TABLE_LEVEL: return AccessFlags::allFlagsGrantableOnTableLevel();
            case COLUMN_LEVEL: return AccessFlags::allFlagsGrantableOnColumnLevel();
        }
        __builtin_unreachable();
    }
}


struct AccessRights::Node
{
public:
    std::shared_ptr<const String> node_name;
    Level level = GLOBAL_LEVEL;
    AccessFlags flags;                   /// flags = (inherited_flags - partial_revokes) | explicit_grants
    AccessFlags min_flags_with_children; /// min_flags_with_children = access & child[0].min_flags_with_children & ... & child[N-1].min_flags_with_children
    AccessFlags max_flags_with_children; /// max_flags_with_children = access | child[0].max_flags_with_children | ... | child[N-1].max_flags_with_children
    std::unique_ptr<std::unordered_map<std::string_view, Node>> children;

    Node() = default;
    Node(const Node & src) { *this = src; }

    Node & operator =(const Node & src)
    {
        if (this == &src)
            return *this;

        node_name = src.node_name;
        level = src.level;
        flags = src.flags;
        min_flags_with_children = src.min_flags_with_children;
        max_flags_with_children = src.max_flags_with_children;
        if (src.children)
            children = std::make_unique<std::unordered_map<std::string_view, Node>>(*src.children);
        else
            children = nullptr;
        return *this;
    }

    void grant(const AccessFlags & flags_)
    {
        AccessFlags flags_to_add = flags_ & getAllGrantableFlags();
        addGrantsRec(flags_to_add);
        optimizeTree();
    }

    template <typename ... Args>
    void grant(const AccessFlags & flags_, const std::string_view & name, const Args &... subnames)
    {
        auto & child = getChild(name);
        child.grant(flags_, subnames...);
        eraseChildIfPossible(child);
        calculateMinMaxFlags();
    }

    template <typename StringT>
    void grant(const AccessFlags & flags_, const std::vector<StringT> & names)
    {
        for (const auto & name : names)
        {
            auto & child = getChild(name);
            child.grant(flags_);
            eraseChildIfPossible(child);
        }
        calculateMinMaxFlags();
    }

    void revoke(const AccessFlags & flags_)
    {
        removeGrantsRec(flags_);
        optimizeTree();
    }

    template <typename... Args>
    void revoke(const AccessFlags & flags_, const std::string_view & name, const Args &... subnames)
    {
        auto & child = getChild(name);

        child.revoke(flags_, subnames...);
        eraseChildIfPossible(child);
        calculateMinMaxFlags();
    }

    template <typename StringT>
    void revoke(const AccessFlags & flags_, const std::vector<StringT> & names)
    {
        for (const auto & name : names)
        {
            auto & child = getChild(name);
            child.revoke(flags_);
            eraseChildIfPossible(child);
        }
        calculateMinMaxFlags();
    }

    bool isGranted(const AccessFlags & flags_) const
    {
        return min_flags_with_children.contains(flags_);
    }

    template <typename... Args>
    bool isGranted(const AccessFlags & flags_, const std::string_view & name, const Args &... subnames) const
    {
        AccessFlags flags_to_check = flags_ - min_flags_with_children;
        if (!flags_to_check)
            return true;
        if (!max_flags_with_children.contains(flags_to_check))
            return false;

        const Node * child = tryGetChild(name);
        if (child)
            return child->isGranted(flags_to_check, subnames...);
        else
            return flags.contains(flags_to_check);
    }

    template <typename StringT>
    bool isGranted(const AccessFlags & flags_, const std::vector<StringT> & names) const
    {
        AccessFlags flags_to_check = flags_ - min_flags_with_children;
        if (!flags_to_check)
            return true;
        if (!max_flags_with_children.contains(flags_to_check))
            return false;

        for (const auto & name : names)
        {
            const Node * child = tryGetChild(name);
            if (child)
            {
                if (!child->isGranted(flags_to_check, name))
                    return false;
            }
            else
            {
                if (!flags.contains(flags_to_check))
                    return false;
            }
        }
        return true;
    }

    friend bool operator ==(const Node & left, const Node & right)
    {
        if (left.flags != right.flags)
            return false;

        if (!left.children)
            return !right.children;

        if (!right.children)
            return false;
        return *left.children == *right.children;
    }

    friend bool operator!=(const Node & left, const Node & right) { return !(left == right); }

    void makeUnion(const Node & other)
    {
        makeUnionRec(other);
        optimizeTree();
    }

    void makeIntersection(const Node & other)
    {
        makeIntersectionRec(other);
        optimizeTree();
    }

    ProtoElements getElements() const
    {
        ProtoElements res;
        getElementsRec(res, {}, *this, {});
        return res;
    }

    static ProtoElements getElements(const Node * node, const Node * node_with_grant_option)
    {
        ProtoElements res;
        getElementsRec(res, {}, node, {}, node_with_grant_option, {});
        return res;
    }

    void modifyFlags(const ModifyFlagsFunction & function, bool & flags_added, bool & flags_removed)
    {
        flags_added = false;
        flags_removed = false;
        modifyFlagsRec(function, flags_added, flags_removed);
        if (flags_added || flags_removed)
            optimizeTree();
    }

    void logTree(Poco::Logger * log, const String & title) const
    {
        LOG_TRACE(log, "Tree({}): level={}, name={}, flags={}, min_flags={}, max_flags={}, num_children={}",
            title, level, node_name ? *node_name : "NULL", flags.toString(),
            min_flags_with_children.toString(), max_flags_with_children.toString(),
            (children ? children->size() : 0));

        if (children)
        {
            for (auto & child : *children | boost::adaptors::map_values)
                child.logTree(log, title);
        }
    }

private:
    AccessFlags getAllGrantableFlags() const { return ::DB::getAllGrantableFlags(level); }
    AccessFlags getChildAllGrantableFlags() const { return ::DB::getAllGrantableFlags(static_cast<Level>(level + 1)); }

    Node * tryGetChild(const std::string_view & name) const
    {
        if (!children)
            return nullptr;
        auto it = children->find(name);
        if (it == children->end())
            return nullptr;
        return &it->second;
    }

    Node & getChild(const std::string_view & name)
    {
        auto * child = tryGetChild(name);
        if (child)
            return *child;
        if (!children)
            children = std::make_unique<std::unordered_map<std::string_view, Node>>();
        auto new_child_name = std::make_shared<const String>(name);
        Node & new_child = (*children)[*new_child_name];
        new_child.node_name = std::move(new_child_name);
        new_child.level = static_cast<Level>(level + 1);
        new_child.flags = flags & new_child.getAllGrantableFlags();
        return new_child;
    }

    void eraseChildIfPossible(Node & child)
    {
        if (!canEraseChild(child))
            return;
        auto it = children->find(*child.node_name);
        children->erase(it);
        if (children->empty())
            children = nullptr;
    }

    bool canEraseChild(const Node & child) const
    {
        return ((flags & child.getAllGrantableFlags()) == child.flags) && !child.children;
    }

    void addGrantsRec(const AccessFlags & flags_)
    {
        if (auto flags_to_add = flags_ & getAllGrantableFlags())
        {
            flags |= flags_to_add;
            if (children)
            {
                for (auto it = children->begin(); it != children->end();)
                {
                    auto & child = it->second;
                    child.addGrantsRec(flags_to_add);
                    if (canEraseChild(child))
                        it = children->erase(it);
                    else
                        ++it;
                }
                if (children->empty())
                    children = nullptr;
            }
        }
    }

    void removeGrantsRec(const AccessFlags & flags_)
    {
        flags &= ~flags_;
        if (children)
        {
            for (auto it = children->begin(); it != children->end();)
            {
                auto & child = it->second;
                child.removeGrantsRec(flags_);
                if (canEraseChild(child))
                    it = children->erase(it);
                else
                    ++it;
            }
            if (children->empty())
                children = nullptr;
        }
    }

    static void getElementsRec(
        ProtoElements & res,
        const boost::container::small_vector<std::string_view, 3> & full_name,
        const Node & node,
        const AccessFlags & parent_flags)
    {
        auto flags = node.flags;
        auto parent_fl = parent_flags & node.getAllGrantableFlags();
        auto revokes = parent_fl - flags;
        auto grants = flags - parent_fl;

        if (revokes)
            res.push_back(ProtoElement{revokes, full_name, false, true});

        if (grants)
            res.push_back(ProtoElement{grants, full_name, false, false});

        if (node.children)
        {
            for (const auto & [child_name, child] : *node.children)
            {
                boost::container::small_vector<std::string_view, 3> child_full_name = full_name;
                child_full_name.push_back(child_name);
                getElementsRec(res, child_full_name, child, flags);
            }
        }
    }

    static void getElementsRec(
        ProtoElements & res,
        const boost::container::small_vector<std::string_view, 3> & full_name,
        const Node * node,
        const AccessFlags & parent_flags,
        const Node * node_go,
        const AccessFlags & parent_flags_go)
    {
        auto grantable_flags = ::DB::getAllGrantableFlags(static_cast<Level>(full_name.size()));
        auto parent_fl = parent_flags & grantable_flags;
        auto parent_fl_go = parent_flags_go & grantable_flags;
        auto flags = node ? node->flags : parent_fl;
        auto flags_go = node_go ? node_go->flags : parent_fl_go;
        auto revokes = parent_fl - flags;
        auto revokes_go = parent_fl_go - flags_go - revokes;
        auto grants_go = flags_go - parent_fl_go;
        auto grants = flags - parent_fl - grants_go;

        if (revokes)
            res.push_back(ProtoElement{revokes, full_name, false, true});

        if (revokes_go)
            res.push_back(ProtoElement{revokes_go, full_name, true, true});

        if (grants)
            res.push_back(ProtoElement{grants, full_name, false, false});

        if (grants_go)
            res.push_back(ProtoElement{grants_go, full_name, true, false});

        if (node && node->children)
        {
            for (const auto & [child_name, child] : *node->children)
            {
                boost::container::small_vector<std::string_view, 3> child_full_name = full_name;
                child_full_name.push_back(child_name);
                const Node * child_node = &child;
                const Node * child_node_go = nullptr;
                if (node_go && node_go->children)
                {
                    auto it = node_go->children->find(child_name);
                    if (it != node_go->children->end())
                        child_node_go = &it->second;
                }
                getElementsRec(res, child_full_name, child_node, flags, child_node_go, flags_go);
            }

        }
        if (node_go && node_go->children)
        {
            for (const auto & [child_name, child] : *node_go->children)
            {
                if (node && node->children && node->children->count(child_name))
                    continue; /// already processed
                boost::container::small_vector<std::string_view, 3> child_full_name = full_name;
                child_full_name.push_back(child_name);
                const Node * child_node = nullptr;
                const Node * child_node_go = &child;
                getElementsRec(res, child_full_name, child_node, flags, child_node_go, flags_go);
            }
        }
    }

    void optimizeTree()
    {
        /// Traverse tree.
        if (children)
        {
            for (auto it = children->begin(); it != children->end();)
            {
                auto & child = it->second;
                child.optimizeTree();
                if (canEraseChild(child))
                    it = children->erase(it);
                else
                    ++it;
            }
            if (children->empty())
                children = nullptr;
        }

        calculateMinMaxFlags();
    }

    void calculateMinMaxFlags()
    {
        /// Calculate min & max access:
        /// min_flags_with_children = access & child[0].min_flags_with_children & ... & child[N-1].min_flags_with_children
        /// max_flags_with_children = access | child[0].max_flags_with_children | ... | child[N-1].max_flags_with_children

        min_flags_with_children = flags;
        max_flags_with_children = flags;
        if (!children || children->empty())
            return;

        AccessFlags min_among_children = AccessFlags::allFlags();
        AccessFlags max_among_children;
        for (const auto & child : *children | boost::adaptors::map_values)
        {
            min_among_children &= child.min_flags_with_children;
            max_among_children |= child.max_flags_with_children;
        }

        max_flags_with_children |= max_among_children;
        AccessFlags add_flags = getAllGrantableFlags() - getChildAllGrantableFlags();
        min_flags_with_children &= min_among_children | add_flags;
    }

    void makeUnionRec(const Node & rhs)
    {
        if (rhs.children)
        {
            for (const auto & [rhs_childname, rhs_child] : *rhs.children)
                getChild(rhs_childname).makeUnionRec(rhs_child);
        }
        flags |= rhs.flags;
        if (children)
        {
            for (auto & [lhs_childname, lhs_child] : *children)
            {
                if (!rhs.tryGetChild(lhs_childname))
                    lhs_child.addGrantsRec(rhs.flags);
            }
        }
    }

    void makeIntersectionRec(const Node & rhs)
    {
        if (rhs.children)
        {
            for (const auto & [rhs_childname, rhs_child] : *rhs.children)
                getChild(rhs_childname).makeIntersectionRec(rhs_child);
        }
        flags &= rhs.flags;
        if (children)
        {
            for (auto & [lhs_childname, lhs_child] : *children)
            {
                if (!rhs.tryGetChild(lhs_childname))
                    lhs_child.removeGrantsRec(~rhs.flags);
            }
        }
    }

    template <typename ... ParentNames>
    void modifyFlagsRec(const ModifyFlagsFunction & function, bool & flags_added, bool & flags_removed, const ParentNames & ... parent_names)
    {
        auto invoke = [&function](const AccessFlags & flags_, const AccessFlags & min_flags_with_children_, const AccessFlags & max_flags_with_children_, std::string_view database_ = {}, std::string_view table_ = {}, std::string_view column_ = {}) -> AccessFlags
        {
            return function(flags_, min_flags_with_children_, max_flags_with_children_, database_, table_, column_);
        };

        if constexpr (sizeof...(ParentNames) < 3)
        {
            if (children)
            {
                for (auto & child : *children | boost::adaptors::map_values)
                {
                    const String & child_name = *child.node_name;
                    child.modifyFlagsRec(function, flags_added, flags_removed, parent_names..., child_name);
                }
            }
        }

        calculateMinMaxFlags();

        auto new_flags = invoke(flags, min_flags_with_children, max_flags_with_children, parent_names...);

        if (new_flags != flags)
        {
            new_flags &= getAllGrantableFlags();
            flags_added |= static_cast<bool>(new_flags - flags);
            flags_removed |= static_cast<bool>(flags - new_flags);
            flags = new_flags;
            calculateMinMaxFlags();
        }
    }
};


AccessRights::AccessRights() = default;
AccessRights::~AccessRights() = default;
AccessRights::AccessRights(AccessRights && src) noexcept = default;
AccessRights & AccessRights::operator =(AccessRights && src) noexcept = default;


AccessRights::AccessRights(const AccessRights & src)
{
    *this = src;
}


AccessRights & AccessRights::operator =(const AccessRights & src)
{
    if (src.root)
        root = std::make_unique<Node>(*src.root);
    else
        root = nullptr;
    if (src.root_with_grant_option)
        root_with_grant_option = std::make_unique<Node>(*src.root_with_grant_option);
    else
        root_with_grant_option = nullptr;
    return *this;
}


AccessRights::AccessRights(const AccessFlags & access)
{
    grant(access);
}


bool AccessRights::isEmpty() const
{
    return !root && !root_with_grant_option;
}


void AccessRights::clear()
{
    root = nullptr;
    root_with_grant_option = nullptr;
}


template <bool with_grant_option, typename... Args>
void AccessRights::grantImpl(const AccessFlags & flags, const Args &... args)
{
    auto helper = [&](std::unique_ptr<Node> & root_node)
    {
        if (!root_node)
            root_node = std::make_unique<Node>();
        root_node->grant(flags, args...);
        if (!root_node->flags && !root_node->children)
            root_node = nullptr;
    };
    helper(root);

    if constexpr (with_grant_option)
        helper(root_with_grant_option);
}

template <bool with_grant_option>
void AccessRights::grantImplHelper(const AccessRightsElement & element)
{
    assert(!element.is_partial_revoke);
    assert(!element.grant_option || with_grant_option);
    if (element.any_database)
        grantImpl<with_grant_option>(element.access_flags);
    else if (element.any_table)
        grantImpl<with_grant_option>(element.access_flags, element.database);
    else if (element.any_column)
        grantImpl<with_grant_option>(element.access_flags, element.database, element.table);
    else
        grantImpl<with_grant_option>(element.access_flags, element.database, element.table, element.columns);
}

template <bool with_grant_option>
void AccessRights::grantImpl(const AccessRightsElement & element)
{
    if (element.is_partial_revoke)
        throw Exception("A partial revoke should be revoked, not granted", ErrorCodes::BAD_ARGUMENTS);
    if constexpr (with_grant_option)
    {
        grantImplHelper<true>(element);
    }
    else
    {
        if (element.grant_option)
            grantImplHelper<true>(element);
        else
            grantImplHelper<false>(element);
    }
}

template <bool with_grant_option>
void AccessRights::grantImpl(const AccessRightsElements & elements)
{
    for (const auto & element : elements)
        grantImpl<with_grant_option>(element);
}

void AccessRights::grant(const AccessFlags & flags) { grantImpl<false>(flags); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database) { grantImpl<false>(flags, database); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) { grantImpl<false>(flags, database, table); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) { grantImpl<false>(flags, database, table, column); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { grantImpl<false>(flags, database, table, columns); }
void AccessRights::grant(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) { grantImpl<false>(flags, database, table, columns); }
void AccessRights::grant(const AccessRightsElement & element) { grantImpl<false>(element); }
void AccessRights::grant(const AccessRightsElements & elements) { grantImpl<false>(elements); }

void AccessRights::grantWithGrantOption(const AccessFlags & flags) { grantImpl<true>(flags); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, const std::string_view & database) { grantImpl<true>(flags, database); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) { grantImpl<true>(flags, database, table); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) { grantImpl<true>(flags, database, table, column); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { grantImpl<true>(flags, database, table, columns); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) { grantImpl<true>(flags, database, table, columns); }
void AccessRights::grantWithGrantOption(const AccessRightsElement & element) { grantImpl<true>(element); }
void AccessRights::grantWithGrantOption(const AccessRightsElements & elements) { grantImpl<true>(elements); }


template <bool grant_option, typename... Args>
void AccessRights::revokeImpl(const AccessFlags & flags, const Args &... args)
{
    auto helper = [&](std::unique_ptr<Node> & root_node)
    {
        if (!root_node)
            return;
        root_node->revoke(flags, args...);
        if (!root_node->flags && !root_node->children)
            root_node = nullptr;
    };
    helper(root_with_grant_option);

    if constexpr (!grant_option)
        helper(root);
}

template <bool grant_option>
void AccessRights::revokeImplHelper(const AccessRightsElement & element)
{
    assert(!element.grant_option || grant_option);
    if (element.any_database)
        revokeImpl<grant_option>(element.access_flags);
    else if (element.any_table)
        revokeImpl<grant_option>(element.access_flags, element.database);
    else if (element.any_column)
        revokeImpl<grant_option>(element.access_flags, element.database, element.table);
    else
        revokeImpl<grant_option>(element.access_flags, element.database, element.table, element.columns);
}

template <bool grant_option>
void AccessRights::revokeImpl(const AccessRightsElement & element)
{
    if constexpr (grant_option)
    {
        revokeImplHelper<true>(element);
    }
    else
    {
        if (element.grant_option)
            revokeImplHelper<true>(element);
        else
            revokeImplHelper<false>(element);
    }
}

template <bool grant_option>
void AccessRights::revokeImpl(const AccessRightsElements & elements)
{
    for (const auto & element : elements)
        revokeImpl<grant_option>(element);
}

void AccessRights::revoke(const AccessFlags & flags) { revokeImpl<false>(flags); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database) { revokeImpl<false>(flags, database); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) { revokeImpl<false>(flags, database, table); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl<false>(flags, database, table, column); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl<false>(flags, database, table, columns); }
void AccessRights::revoke(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl<false>(flags, database, table, columns); }
void AccessRights::revoke(const AccessRightsElement & element) { revokeImpl<false>(element); }
void AccessRights::revoke(const AccessRightsElements & elements) { revokeImpl<false>(elements); }

void AccessRights::revokeGrantOption(const AccessFlags & flags) { revokeImpl<true>(flags); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, const std::string_view & database) { revokeImpl<true>(flags, database); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) { revokeImpl<true>(flags, database, table); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl<true>(flags, database, table, column); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl<true>(flags, database, table, columns); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl<true>(flags, database, table, columns); }
void AccessRights::revokeGrantOption(const AccessRightsElement & element) { revokeImpl<true>(element); }
void AccessRights::revokeGrantOption(const AccessRightsElements & elements) { revokeImpl<true>(elements); }


AccessRightsElements AccessRights::getElements() const
{
#if 0
    logTree();
#endif
    if (!root)
        return {};
    if (!root_with_grant_option)
        return root->getElements().getResult();
    return Node::getElements(root.get(), root_with_grant_option.get()).getResult();
}


String AccessRights::toString() const
{
    return getElements().toString();
}


template <bool grant_option, typename... Args>
bool AccessRights::isGrantedImpl(const AccessFlags & flags, const Args &... args) const
{
    auto helper = [&](const std::unique_ptr<Node> & root_node) -> bool
    {
        if (!root_node)
            return flags.isEmpty();
        return root_node->isGranted(flags, args...);
    };
    if constexpr (grant_option)
        return helper(root_with_grant_option);
    else
        return helper(root);
}

template <bool grant_option>
bool AccessRights::isGrantedImplHelper(const AccessRightsElement & element) const
{
    assert(!element.grant_option || grant_option);
    if (element.any_database)
        return isGrantedImpl<grant_option>(element.access_flags);
    else if (element.any_table)
        return isGrantedImpl<grant_option>(element.access_flags, element.database);
    else if (element.any_column)
        return isGrantedImpl<grant_option>(element.access_flags, element.database, element.table);
    else
        return isGrantedImpl<grant_option>(element.access_flags, element.database, element.table, element.columns);
}

template <bool grant_option>
bool AccessRights::isGrantedImpl(const AccessRightsElement & element) const
{
    if constexpr (grant_option)
    {
        return isGrantedImplHelper<true>(element);
    }
    else
    {
        if (element.grant_option)
            return isGrantedImplHelper<true>(element);
        else
            return isGrantedImplHelper<false>(element);
    }
}

template <bool grant_option>
bool AccessRights::isGrantedImpl(const AccessRightsElements & elements) const
{
    for (const auto & element : elements)
        if (!isGrantedImpl<grant_option>(element))
            return false;
    return true;
}

bool AccessRights::isGranted(const AccessFlags & flags) const { return isGrantedImpl<false>(flags); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database) const { return isGrantedImpl<false>(flags, database); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return isGrantedImpl<false>(flags, database, table); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return isGrantedImpl<false>(flags, database, table, column); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return isGrantedImpl<false>(flags, database, table, columns); }
bool AccessRights::isGranted(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return isGrantedImpl<false>(flags, database, table, columns); }
bool AccessRights::isGranted(const AccessRightsElement & element) const { return isGrantedImpl<false>(element); }
bool AccessRights::isGranted(const AccessRightsElements & elements) const { return isGrantedImpl<false>(elements); }

bool AccessRights::hasGrantOption(const AccessFlags & flags) const { return isGrantedImpl<true>(flags); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, const std::string_view & database) const { return isGrantedImpl<true>(flags, database); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return isGrantedImpl<true>(flags, database, table); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return isGrantedImpl<true>(flags, database, table, column); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return isGrantedImpl<true>(flags, database, table, columns); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return isGrantedImpl<true>(flags, database, table, columns); }
bool AccessRights::hasGrantOption(const AccessRightsElement & element) const { return isGrantedImpl<true>(element); }
bool AccessRights::hasGrantOption(const AccessRightsElements & elements) const { return isGrantedImpl<true>(elements); }


bool operator ==(const AccessRights & left, const AccessRights & right)
{
    auto helper = [](const std::unique_ptr<AccessRights::Node> & left_node, const std::unique_ptr<AccessRights::Node> & right_node)
    {
        if (!left_node)
            return !right_node;
        if (!right_node)
            return false;
        return *left_node == *right_node;
    };
    return helper(left.root, right.root) && helper(left.root_with_grant_option, right.root_with_grant_option);
}


void AccessRights::makeUnion(const AccessRights & other)
{
    auto helper = [](std::unique_ptr<Node> & root_node, const std::unique_ptr<Node> & other_root_node)
    {
        if (!root_node)
        {
            if (other_root_node)
                root_node = std::make_unique<Node>(*other_root_node);
            return;
        }
        if (other_root_node)
        {
            root_node->makeUnion(*other_root_node);
            if (!root_node->flags && !root_node->children)
                root_node = nullptr;
        }
    };
    helper(root, other.root);
    helper(root_with_grant_option, other.root_with_grant_option);
}


void AccessRights::makeIntersection(const AccessRights & other)
{
    auto helper = [](std::unique_ptr<Node> & root_node, const std::unique_ptr<Node> & other_root_node)
    {
        if (!root_node)
            return;
        if (!other_root_node)
        {
            root_node = nullptr;
            return;
        }
        root_node->makeIntersection(*other_root_node);
        if (!root_node->flags && !root_node->children)
            root_node = nullptr;
    };
    helper(root, other.root);
    helper(root_with_grant_option, other.root_with_grant_option);
}


void AccessRights::modifyFlags(const ModifyFlagsFunction & function)
{
    if (!root)
        return;
    bool flags_added, flags_removed;
    root->modifyFlags(function, flags_added, flags_removed);
    if (flags_removed && root_with_grant_option)
        root_with_grant_option->makeIntersection(*root);
}


void AccessRights::modifyFlagsWithGrantOption(const ModifyFlagsFunction & function)
{
    if (!root_with_grant_option)
        return;
    bool flags_added, flags_removed;
    root_with_grant_option->modifyFlags(function, flags_added, flags_removed);
    if (flags_added)
    {
        if (!root)
            root = std::make_unique<Node>();
        root->makeUnion(*root_with_grant_option);
    }
}


AccessRights AccessRights::getFullAccess()
{
    AccessRights res;
    res.grantWithGrantOption(AccessType::ALL);
    return res;
}


void AccessRights::logTree() const
{
    auto * log = &Poco::Logger::get("AccessRights");
    if (root)
    {
        root->logTree(log, "");
        if (root_with_grant_option)
            root->logTree(log, "go");
    }
    else
        LOG_TRACE(log, "Tree: NULL");
}
}
