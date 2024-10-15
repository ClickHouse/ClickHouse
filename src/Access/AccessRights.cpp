#include <Access/AccessRights.h>
#include <base/sort.h>
#include <Common/Exception.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <boost/container/small_vector.hpp>
#include <list>

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
        boost::container::small_vector<String, 3> full_name;
        bool grant_option = false;
        bool is_partial_revoke = false;
        bool wildcard = false;

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
            res.wildcard = wildcard;
            switch (full_name.size()) // NOLINT(bugprone-switch-missing-default-case)
            {
                case 0:
                    break;
                case 1:
                {
                    if (access_flags.isGlobalWithParameter())
                        res.parameter = full_name[0];
                    else
                        res.database = full_name[0];
                    break;
                }
                case 2:
                {
                    res.database = full_name[0];
                    res.table = full_name[1];
                    break;
                }
                case 3:
                {
                    res.database = full_name[0];
                    res.table = full_name[1];
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
                    const auto & element = sorted[i];
                    if (element.access_flags)
                    {
                        const bool all_granted = sorted.size() == 1 && element.access_flags.contains(AccessFlags::allFlags());
                        if (all_granted)
                        {
                            /// Easy case: one Element is converted to one AccessRightsElement.
                            res.emplace_back(element.getResult());
                        }
                        else
                        {
                            auto per_parameter = element.access_flags.splitIntoParameterTypes();
                            if (per_parameter.size() == 1)
                            {
                                /// Easy case: one Element is converted to one AccessRightsElement.
                                res.emplace_back(element.getResult());
                            }
                            else
                            {
                                /// Difficult case: one element is converted into multiple AccessRightsElements.
                                for (const auto & [_, parameter_flags] : per_parameter)
                                {
                                    auto current_element{element};
                                    current_element.access_flags = parameter_flags;
                                    res.emplace_back(current_element.getResult());
                                }
                            }
                        }
                    }
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
                    || (element.access_flags.isGlobalWithParameter() != start_element.access_flags.isGlobalWithParameter())
                    || (element.access_flags.getParameterType() != start_element.access_flags.getParameterType())
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
                back.database = pbegin->full_name[0];
                back.table = pbegin->full_name[1];
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

    /**
     *  Levels:
     *  1. GLOBAL
     *  2. DATABASE_LEVEL          2. GLOBAL_WITH_PARAMETER (parameter example: named collection)
     *  3. TABLE_LEVEL
     *  4. COLUMN_LEVEL
     */

    enum Level
    {
        GLOBAL_LEVEL = 0,
        DATABASE_LEVEL = 1,
        GLOBAL_WITH_PARAMETER = DATABASE_LEVEL,
        TABLE_LEVEL = 2,
        COLUMN_LEVEL = 3,
        MAX = COLUMN_LEVEL,
    };

    AccessFlags getAllGrantableFlags(Level level)
    {
        switch (level)
        {
            case GLOBAL_LEVEL: return AccessFlags::allFlagsGrantableOnGlobalLevel();
            case DATABASE_LEVEL: return AccessFlags::allFlagsGrantableOnDatabaseLevel() | AccessFlags::allFlagsGrantableOnGlobalWithParameterLevel();
            case TABLE_LEVEL: return AccessFlags::allFlagsGrantableOnTableLevel();
            case COLUMN_LEVEL: return AccessFlags::allFlagsGrantableOnColumnLevel();
        }
        chassert(false);
    }
}

/** This structure represents all access rights for a specific entity in a RADIX Tree format.
  * Each node contains a name and access rights for this path or prefix
  * Example structure:
  *
  * <node1 name="db"> -> <node2 name=""> -> <node3 name="table"> -> <node4 name="">
  *
  * Here, nodes with empty names represent the specific meaningful paths, and other nodes represent prefixes for these paths.
  * Let's assume we have these tables available: db_1.table1, db_2.table, db.table, db.table1, db.foo
  *
  * node1: GRANT ON db*.*      (matches db_1.table1, db_2.table, db.table, db.table1, db.foo)
  * node2: GRANT ON db.*       (matches db.table, db.table1, db.foo)
  * node3: GRANT ON db.table*  (matches db.table, db.table1)
  * node4: GRANT ON db.table   (matches db.table)
  *
  * If too paths have the same prefix, the tree splits in between:
  *
  * GRANT ON team.*
  * GRANT ON test.table
  *
  *           <node1 name="te">
  *              /            \
  *    <node2 name="am">   <node4 name="st">
  *           |                  |
  *     <node3 name="">    <node5 name="">
  *                              |
  *                        <node6 name="table">
  *                              |
  *                        <node7 name="">
  *
  * node1: GRANT ON te*.*
  * node2: GRANT ON team*.*
  * node3: GRANT ON team.*
  * node4: GRANT ON test*.*
  * node5: GRANT ON test.*
  * node6: GRANT ON test.table*
  * node7: GRANT ON test.table
  */
struct AccessRights::Node
{
public:
    using Container = std::list<Node>;

    class Iterator
    {
    public:
        Iterator() = delete;
        explicit Iterator(const Node & node) noexcept : root{node} {}

        bool operator!=(const Iterator & other) const noexcept
        {
            return
                current != other.current;
        }

        const Node & operator*() const noexcept
        {
            return *current;
        }

        const Node * operator->() const noexcept
        {
            return current;
        }

        Iterator & operator++() noexcept
        {
            if (!root.children || root.children->empty())
                return *this;

            if (iters.empty())
                iters.emplace_back(root.children->begin(), root.children->end());

            while (!iters.empty())
            {
                auto & [begin, end] = iters.back();
                if (begin == end)
                {
                    iters.pop_back();
                    if (!iters.empty())
                        ++iters.back().first;
                    continue;
                }

                if (&*begin != current)
                {
                    if (begin->isLeaf() || begin->wildcard_grant)
                    {
                        current = &*begin;
                        return *this;
                    }
                }

                if (!begin->isLeaf() && begin->children)
                    iters.emplace_back(begin->children->begin(), begin->children->end());
                else
                    ++begin;
            }
            current = nullptr;
            return *this;
        }

        String getPath() const
        {
            String res;
            for (const auto & it : iters)
                res += it.first->node_name;
            return res;
        }

    private:
        const Node & root;
        const Node * current = nullptr;
        std::vector<std::pair<Container::iterator, Container::iterator>> iters;
    };

    String node_name;
    Level level = GLOBAL_LEVEL;
    AccessFlags flags;                   /// flags = (inherited_flags - partial_revokes) | explicit_grants
    AccessFlags min_flags_with_children; /// min_flags_with_children = access & child[0].min_flags_with_children & ... & child[N-1].min_flags_with_children
    AccessFlags max_flags_with_children; /// max_flags_with_children = access | child[0].max_flags_with_children | ... | child[N-1].max_flags_with_children
    std::unique_ptr<Container> children;

    bool wildcard_grant = false;

    Node() = default;
    Node(const Node & src) { *this = src; }

    Iterator begin() const { return ++Iterator{*this}; }
    Iterator end() const { return Iterator{*this}; }

    Node & operator =(const Node & src)
    {
        if (this == &src)
            return *this;

        node_name = src.node_name;
        level = src.level;
        flags = src.flags;
        wildcard_grant = src.wildcard_grant;
        min_flags_with_children = src.min_flags_with_children;
        max_flags_with_children = src.max_flags_with_children;
        if (src.children)
            children = std::make_unique<Container>(*src.children);
        else
            children = nullptr;
        return *this;
    }

    bool isLeaf() const { return node_name.empty(); }

    template <bool wildcard = false>
    void grant(const AccessFlags & flags_)
    {
        if constexpr (wildcard)
            wildcard_grant = true;

        AccessFlags flags_to_add = flags_ & getAllGrantableFlags();
        addGrantsRec(flags_to_add);
        optimizeTree();
    }

    template <bool wildcard = false, typename ... Args>
    void grant(const AccessFlags & flags_, std::string_view name, const Args &... subnames)
    {
        auto next_level = static_cast<Level>(level + 1);

        Node * child;
        if constexpr (sizeof...(Args) == 0)
            /// In order to grant/revoke a wildcard grant we need to update flags on the leaf's parent and not the actual leaf.
            child = &getLeaf(name, next_level, /* return_parent_node= */ wildcard);
        else
            child = &getLeaf(name, next_level);

        child->grant<wildcard>(flags_, subnames...);
        optimizePath(name);
    }

    template <bool wildcard = false, typename StringT>
    void grant(const AccessFlags & flags_, const std::vector<StringT> & names)
    {
        for (const auto & name : names)
        {
            auto & child = getLeaf(name, static_cast<Level>(level + 1), wildcard);
            child.grant(flags_);
            optimizePath(name);
        }
    }

    template <bool wildcard = false>
    void revoke(const AccessFlags & flags_)
    {
        if constexpr (wildcard)
            wildcard_grant = true;

        removeGrantsRec(flags_);
        optimizeTree();
    }

    template <bool wildcard = false, typename... Args>
    void revoke(const AccessFlags & flags_, std::string_view name, const Args &... subnames)
    {
        auto next_level = static_cast<Level>(level + 1);

        Node * child;
        if constexpr (sizeof...(Args) == 0)
            /// In order to grant/revoke a wildcard grant we need to update flags on the leaf's parent and not the actual leaf.
            child = &getLeaf(name, next_level, /* return_parent_node= */ wildcard);
        else
            child = &getLeaf(name, next_level);

        child->revoke<wildcard>(flags_, subnames...);
        optimizePath(name);
    }

    template <bool wildcard = false, typename StringT>
    void revoke(const AccessFlags & flags_, const std::vector<StringT> & names)
    {
        for (const auto & name : names)
        {
            auto & child = getLeaf(name, static_cast<Level>(level + 1), wildcard);
            child.revoke(flags_);
            optimizePath(name);
        }
    }

    template <bool wildcard = false>
    bool isGranted(const AccessFlags & flags_) const
    {
        return min_flags_with_children.contains(flags_);
    }

    template <bool wildcard = false, typename... Args>
    bool isGranted(const AccessFlags & flags_, std::string_view name, const Args &... subnames) const
    {
        AccessFlags flags_to_check = flags_ - min_flags_with_children;
        if (!flags_to_check)
            return true;
        if (!max_flags_with_children.contains(flags_to_check))
            return false;

        if constexpr (sizeof...(Args) == 0 && wildcard)
        {
            /// We need to check the closest parent's flags
            ///
            /// Example: GRANT SELECT ON foo*, REVOKE SELECT ON foo
            /// isGranted(SELECT, "foo") == false
            /// isGrantedWildcard(SELECT, "foo") == true
            /// isGrantedWildcard(SELECT, "foobar") == true
            ///
            ///                 "foo" (SELECT)
            ///                /             \
            ///     "" (leaf, USAGE)        "bar" (SELECT)
            const auto & [node, _] = tryGetLeafOrPrefix(name, /* return_parent_node= */ true);
            return node.flags.contains(flags_to_check);
        }

        const auto & [node, final] = tryGetLeafOrPrefix(name);
        if (final)
            return node.isGranted<wildcard>(flags_to_check, subnames...);

        return node.flags.contains(flags_to_check);
    }

    template <bool wildcard = false, typename StringT>
    bool isGranted(const AccessFlags & flags_, const std::vector<StringT> & names) const
    {
        AccessFlags flags_to_check = flags_ - min_flags_with_children;
        if (!flags_to_check)
            return true;
        if (!max_flags_with_children.contains(flags_to_check))
            return false;

        for (const auto & name : names)
        {
            const Node * child = tryGetLeaf(name);
            if (child)
            {
                if (!child->isGranted<wildcard>(flags_to_check, name))
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
        if (left.node_name != right.node_name)
            return false;

        if (left.wildcard_grant != right.wildcard_grant)
            return false;

        if (left.flags != right.flags)
            return false;

        if (!left.children)
            return !right.children;

        if (!right.children)
            return false;
        return *left.children == *right.children;
    }

    friend bool operator!=(const Node & left, const Node & right) { return !(left == right); }

    bool contains(const Node & other) const
    {
        Node tmp_node = *this;
        tmp_node.makeIntersection(other);
        /// If we get the same node after the intersection, our node is fully covered by the given one.
        return tmp_node == other;
    }

    void makeUnion(const Node & other)
    {
        /// We need these tmp nodes because union/intersect operations are use `getLeaf` function which can't be made const.
        /// Potentially, we can use tryGetLeaf, but it's very complicated to traverse both trees at the same time:
        ///
        /// Tree1:
        ///              f (SELECT)
        ///           /            \
        ///        ancy (SELECT)   oo (SELECT)
        ///          |                 |
        ///    "" (SELECT, INSERT)    "" (SELECT, INSERT)
        ///
        /// Tree2:
        ///          foo (SELECT)
        ///              |
        ///         "" (SELECT, INSERT)
        ///
        /// Here Tree2.getLeaf("f") will return a valid node, but Tree2.tryGetLeaf("f") will return nullptr

        Node result;
        Node rhs = other;
        makeUnionRec(result, rhs);
        children = std::move(result.children);
        flags |= other.flags;
        optimizeTree();
    }

    void makeIntersection(const Node & other)
    {
        Node result;
        Node rhs = other;
        makeIntersectionRec(result, rhs);
        children = std::move(result.children);
        flags &= other.flags;
        optimizeTree();
    }

    void makeDifference(const Node & other)
    {
        Node rhs = other;
        makeDifferenceRec(*this, rhs);
        flags -= other.flags;
        optimizeTree();
    }

    ProtoElements getElements() const
    {
        ProtoElements res;
        getElementsRec(res, {}, *this, {}, "");
        return res;
    }

    static ProtoElements getElements(const Node & node, const Node & node_with_grant_option)
    {
        ProtoElements res;
        Node tmp_node = node;
        Node tmp_node_go = node_with_grant_option;
        getElementsRec(res, {}, &tmp_node, {}, &tmp_node_go, {}, "");
        return res;
    }

    void modifyFlags(const ModifyFlagsFunction & function, bool grant_option, bool & flags_added, bool & flags_removed)
    {
        flags_added = false;
        flags_removed = false;
        modifyFlagsRec(function, grant_option, flags_added, flags_removed);
        if (flags_added || flags_removed)
            optimizeTree();
    }

    void dumpTree(WriteBuffer & buffer, const String & title, size_t depth = 0) const
    {
        buffer << fmt::format("Tree({}): {} level={}, name={}, flags={}, min_flags={}, max_flags={}, wildcard_grant={}, num_children={}\n",
            title, String(depth, '-'), level, !isLeaf() ? node_name : "NULL", flags.toString(),
            min_flags_with_children.toString(), max_flags_with_children.toString(), wildcard_grant,
            (children ? children->size() : 0));

        if (children)
        {
            for (auto & child : *children)
                child.dumpTree(buffer, title, depth + 1);
        }
    }

private:
    AccessFlags getAllGrantableFlags() const { return ::DB::getAllGrantableFlags(level); }
    AccessFlags getChildAllGrantableFlags() const { return ::DB::getAllGrantableFlags(static_cast<Level>(level == Level::MAX ? level : (level + 1))); }

    /// Finds a leaf in the prefix tree. If no leaf is found, creates a new one with specified *level_*.
    /// Boolean modifier *return_parent_node* allows to return of a parent node for the intended leaf's path instead of the leaf itself.
    Node & getLeaf(std::string_view path, Level level_ = Level::GLOBAL_LEVEL, bool return_parent_node = false)
    {
        if (path.empty() && return_parent_node)
            return *this;

        if (!children)
            children = std::make_unique<Container>();

        auto find_possible_prefix = [path](const Node & n)
        {
            if (path.empty())
                return n.isLeaf();

            return n.node_name[0] == path[0];
        };

        if (auto it = std::find_if(children->begin(), children->end(), find_possible_prefix); it != children->end())
        {
            auto & child = *it;

            if (path.empty())
                /// This will always return a valid reference because our container is std::list.
                return child;

            const auto & [left, right] = std::mismatch(path.begin(), path.end(), child.node_name.begin(), child.node_name.end());

            /// `name.starts_with(child.node_name)`.
            /// In this case we remove prefix and continue grant from child.
            if (right == child.node_name.end())
                return child.getLeaf(path.substr(child.node_name.size()), level_, return_parent_node);

            /// `name` and `child.node_name` have a mismatch at position `i`.
            /// Now we split original and create the branches with possible suffixes:
            ///
            ///                    st -- >
            ///                  /
            /// team + test = te
            ///                  \
            ///                   am -- >
            size_t i = std::distance(path.begin(), left);
            std::string_view prefix = path.substr(0, i);
            std::string_view new_path = path.substr(i);

            auto & new_parent = getChildNode(prefix, level_);
            child.node_name = child.node_name.substr(i);
            new_parent.children = std::make_unique<Container>();
            new_parent.children->splice(new_parent.children->begin(), *children, it);

            return new_parent.getLeaf(new_path, level_, return_parent_node);
        }

        /// No similar prefix in children was found, now we can just insert the whole path as a leaf.
        auto & child = getChildNode(path, level_);

        /// Child is a leaf.
        if (path.empty())
            return child;

        /// Creates a leaf in child.
        return child.getLeaf("", level_, return_parent_node);
    }

    /// Returns a pair [node, is_leaf].
    std::pair<const Node &, bool> tryGetLeafOrPrefix(std::string_view path, bool return_parent_node = false) const
    {
        if (!children)
            return {*this, false};

        for (auto & child : *children)
        {
            if (path.empty() && child.isLeaf())
                return {child, true};

            if (!child.isLeaf() && path.starts_with(child.node_name))
            {
                if (return_parent_node && path == child.node_name)
                    return {child, true};

                return child.tryGetLeafOrPrefix(path.substr(child.node_name.size()));
            }
        }

        return {*this, false};
    }

    /// Similar to `getLeaf`, but returns nullptr if no leaf was found.
    const Node * tryGetLeaf(std::string_view path, bool return_parent_node = false) const
    {
        const auto & [node, final] = tryGetLeafOrPrefix(path, return_parent_node);
        if (!final)
            return nullptr;

        return &node;
    }

    /// Returns a child node with the given name. If no child was found, creates a new one with specified *level_*.
    Node & getChildNode(std::string_view name, Level level_ = Level::GLOBAL_LEVEL)
    {
        auto * child = tryGetChildNode(name);
        if (child)
            return *child;

        if (!children)
            children = std::make_unique<Container>();

        auto & new_child = children->emplace_back();
        new_child.node_name = name;
        new_child.level = level_;
        new_child.flags = flags & new_child.getAllGrantableFlags();

        return new_child;
    }

    /// Similar to `getChildNode`, but returns nullptr if no child node was found.
    Node * tryGetChildNode(std::string_view name) const
    {
        if (!children)
            return nullptr;
        auto it = std::find_if(children->begin(), children->end(), [name](const auto & n) { return n.node_name == name; });
        if (it == children->end())
            return nullptr;
        return &*it;
    }

    bool canMergeChild(const Node & child) const
    {
        return ((flags & child.getAllGrantableFlags()) == child.flags);
    }

    bool canEraseChild(const Node & child) const
    {
        return canMergeChild(child) && !child.children;
    }

    void addGrantsRec(const AccessFlags & flags_)
    {
        if (auto flags_to_add = flags_ & getAllGrantableFlags())
        {
            flags |= flags_to_add;
            if (children)
            {
                for (auto & child : *children)
                    child.addGrantsRec(flags_to_add);
            }
        }
    }

    void removeGrantsRec(const AccessFlags & flags_)
    {
        flags &= ~flags_;
        if (children)
        {
            for (auto & child : *children)
                child.removeGrantsRec(flags_);
        }
    }

    /// Dumps GRANT/REVOKE elements from the tree.
    static void getElementsRec(
        ProtoElements & res,
        boost::container::small_vector<String, 3> full_name,
        const Node & node,
        const AccessFlags & parent_flags,
        String path)
    {
        auto flags = node.flags;
        auto parent_fl = parent_flags & node.getAllGrantableFlags();
        auto revokes = parent_fl - flags;
        auto grants = flags - parent_fl;

        /// Inserts into result only meaningful nodes (e.g. wildcards or leafs).
        if (node.isLeaf() || node.wildcard_grant)
        {
            boost::container::small_vector<String, 3> new_full_name = full_name;

            if (node.level != Level::GLOBAL_LEVEL)
            {
                new_full_name.push_back(path);

                /// If in leaf, flushes the current path into `full_name`.
                if (node.isLeaf())
                {
                    full_name.push_back(path);
                    path.clear();
                }
            }

            if (revokes)
                res.push_back(ProtoElement{revokes, new_full_name, false, true, node.wildcard_grant});

            if (grants)
                res.push_back(ProtoElement{grants, new_full_name, false, false, node.wildcard_grant});
        }

        if (node.children)
        {
            for (const auto & child : *node.children)
            {
                String new_path = path;
                new_path.append(child.node_name);
                getElementsRec(res, full_name, child, flags, new_path);
            }
        }
    }

    /// Dumps GRANT/REVOKE elements from the tree with grant options.
    static void getElementsRec(
        ProtoElements & res,
        boost::container::small_vector<String, 3> full_name,
        Node * node,
        const AccessFlags & parent_flags,
        Node * node_go,
        const AccessFlags & parent_flags_go,
        String path)
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

        Node * target_node = node;
        if (!target_node)
            target_node = node_go;

        /// Inserts into result only meaningful nodes (e.g. wildcards or leafs).
        if (target_node && (target_node->isLeaf() || target_node->wildcard_grant))
        {
            boost::container::small_vector<String, 3> new_full_name = full_name;

            if (target_node->level != Level::GLOBAL_LEVEL)
            {
                new_full_name.push_back(path);

                /// If in leaf, flushes the current path into `full_name`.
                if (target_node->isLeaf())
                {
                    full_name.push_back(path);
                    path.clear();
                }
            }

            if (node && revokes)
                res.push_back(ProtoElement{revokes, new_full_name, false, true, node->wildcard_grant});

            if (node_go && revokes_go)
                res.push_back(ProtoElement{revokes_go, new_full_name, true, true, node_go->wildcard_grant});

            if (node && grants)
                res.push_back(ProtoElement{grants, new_full_name, false, false, node->wildcard_grant});

            if (node_go && grants_go)
                res.push_back(ProtoElement{grants_go, new_full_name, true, false, node_go->wildcard_grant});
        }

        if (node && node->children)
        {
            for (auto & child : *node->children)
            {
                String new_path = path;
                new_path.append(child.node_name);

                Node * child_node = &child;
                Node * child_node_go = nullptr;
                if (node_go)
                    child_node_go = &node_go->getLeaf(child.node_name, child.level, !child.isLeaf());
                getElementsRec(res, full_name, child_node, flags, child_node_go, flags_go, new_path);
            }

        }
        if (node_go && node_go->children)
        {
            for (auto & child : *node_go->children)
            {
                if (node && node->children)
                {
                    auto starts_with = [&child](const Node & n)
                    {
                        if (child.isLeaf())
                            return n.isLeaf();

                        return !n.isLeaf() && child.node_name[0] == n.node_name[0];
                    };

                    if (auto it = std::find_if(node->children->begin(), node->children->end(), starts_with); it != node->children->end())
                        continue; /// already processed
                }

                String new_path = path;
                new_path.append(child.node_name);
                Node * child_node = nullptr;
                Node * child_node_go = &child;
                getElementsRec(res, full_name, child_node, flags, child_node_go, flags_go, new_path);
            }
        }
    }


    void optimizeChildren()
    {
        if (!children)
            return;

        for (auto it = children->begin(); it != children->end();)
        {
            /// If a child has a wildcard grant, but it's fully covered by parent grants.
            /// Example:
            ///
            /// GRANT SELECT ON db.tb* |
            ///                        | -> GRANT SELECT ON db.*
            /// GRANT SELECT ON db.*   |
            if (it->wildcard_grant && canMergeChild(*it))
            {
                it->wildcard_grant = false;
                it->optimizeChildren();
            }

            if (canEraseChild(*it))
                it = children->erase(it);
            else
                ++it;
        }

        /// When we have only one child left after optimization, we must perform a radix compression
        ///
        /// toast-er -> toaster
        if (children->size() == 1)
        {
            auto it = children->begin();

            if (!isLeaf() && !it->isLeaf() && canMergeChild(*it) && !wildcard_grant)
            {
                node_name.append(it->node_name.begin(), it->node_name.end());

                /// This will trigger the smart pointer destruction, which will invalidate `it`.
                /// But since we already moved `children`, this should work like a `swap`.
                children = std::move(it->children);
            }
        }

        if (children && children->empty())
            children = nullptr;
    }

    /// Optimizes and compresses radix tree.
    void optimizeTree()
    {
        if (children)
        {
            for (auto & child : *children)
                child.optimizeTree();

            optimizeChildren();
        }

        calculateMinMaxFlags();
    }


    /// Works similar to `optimizeTree`, but affects only specified path (and subpaths).
    /// Can be used as a minor optimization in GRANT/REVOKE when we want optimize only affected parts of the tree.
    void optimizePath(std::string_view path)
    {
        if (children)
        {
            for (auto & child : *children)
            {
                if (path.empty() && child.isLeaf())
                    break;

                if (!child.isLeaf() && path.starts_with(child.node_name))
                {
                    if (!path.empty())
                        child.optimizePath(path.substr(child.node_name.size()));

                    break;
                }
            }

            optimizeChildren();
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
        for (const auto & child : *children)
        {
            min_among_children &= child.min_flags_with_children;
            max_among_children |= child.max_flags_with_children;
        }

        max_flags_with_children |= max_among_children;

        AccessFlags add_flags;
        if (level != Level::COLUMN_LEVEL && isLeaf())
            add_flags = getAllGrantableFlags() - getChildAllGrantableFlags();

        min_flags_with_children &= min_among_children | add_flags;
    }

    void makeUnionRec(Node & result, Node & rhs)
    {
        if (rhs.children)
        {
            for (auto & rhs_child : *rhs.children)
            {
                auto & result_child = result.getLeaf(rhs_child.node_name, rhs_child.level, !rhs_child.isLeaf());
                auto & lhs_child = getLeaf(rhs_child.node_name, rhs_child.level, !rhs_child.isLeaf());
                lhs_child.makeUnionRec(result_child, rhs_child);
            }
        }

        if (children)
        {
            for (auto & lhs_child : *children)
            {
                auto & result_child = result.getLeaf(lhs_child.node_name, lhs_child.level, !lhs_child.isLeaf());
                auto & rhs_child = rhs.getLeaf(lhs_child.node_name, lhs_child.level, !lhs_child.isLeaf());
                rhs_child.makeUnionRec(result_child, lhs_child);
            }
        }

        result.flags = flags | rhs.flags;
        result.wildcard_grant = wildcard_grant || rhs.wildcard_grant;
    }

    void makeIntersectionRec(Node & result, Node & rhs)
    {
        if (rhs.children)
        {
            for (auto & rhs_child : *rhs.children)
            {
                auto & result_child = result.getLeaf(rhs_child.node_name, rhs_child.level, !rhs_child.isLeaf());
                auto & lhs_child = getLeaf(rhs_child.node_name, rhs_child.level, !rhs_child.isLeaf());
                lhs_child.makeIntersectionRec(result_child, rhs_child);
            }
        }

        if (children)
        {
            for (auto & lhs_child : *children)
            {
                auto & result_child = result.getLeaf(lhs_child.node_name, lhs_child.level, !lhs_child.isLeaf());
                auto & rhs_child = rhs.getLeaf(lhs_child.node_name, lhs_child.level, !lhs_child.isLeaf());
                rhs_child.makeIntersectionRec(result_child, lhs_child);
            }
        }

        result.flags = flags & rhs.flags;
        /// This will produce more wildcard grants than expected, but all of them will be trivial and will be reduced by the `optimizeChildren`
        result.wildcard_grant = wildcard_grant || rhs.wildcard_grant;
    }

    void makeDifferenceRec(Node & result, Node & rhs)
    {
        if (rhs.children)
        {
            for (auto & rhs_child : *rhs.children)
            {
                auto & result_child = result.getLeaf(rhs_child.node_name, rhs_child.level, !rhs_child.isLeaf());
                auto & lhs_child = getLeaf(rhs_child.node_name, rhs_child.level, !rhs_child.isLeaf());
                lhs_child.makeDifferenceRec(result_child, rhs_child);
            }
        }

        if (children)
        {
            for (auto & lhs_child : *children)
            {
                auto & result_child = result.getLeaf(lhs_child.node_name, lhs_child.level, !lhs_child.isLeaf());
                auto & rhs_child = rhs.getLeaf(lhs_child.node_name, lhs_child.level, !lhs_child.isLeaf());
                lhs_child.makeDifferenceRec(result_child, rhs_child);
            }
        }

        result.flags = flags - rhs.flags;
        result.wildcard_grant = wildcard_grant || rhs.wildcard_grant;
    }

    void modifyFlagsRec(const ModifyFlagsFunction & function, bool grant_option, bool & flags_added, bool & flags_removed)
    {
        if (children)
        {
            for (auto & child : *children)
            {
                child.modifyFlagsRec(function, grant_option, flags_added, flags_removed);
            }
        }

        calculateMinMaxFlags();

        auto new_flags = function(flags, min_flags_with_children, max_flags_with_children, level, grant_option);

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


AccessRights::AccessRights(const AccessRightsElement & element)
{
    grant(element);
}


AccessRights::AccessRights(const AccessRightsElements & elements)
{
    grant(elements);
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


template <bool with_grant_option, bool wildcard, typename... Args>
void AccessRights::grantImpl(const AccessFlags & flags, const Args &... args)
{
    auto helper = [&](std::unique_ptr<Node> & root_node)
    {
        if (!root_node)
            root_node = std::make_unique<Node>();
        root_node->grant<wildcard>(flags, args...);
        if (!root_node->flags && !root_node->children)
            root_node = nullptr;
    };
    helper(root);

    if constexpr (with_grant_option)
        helper(root_with_grant_option);
}

template <bool with_grant_option, bool wildcard>
void AccessRights::grantImplHelper(const AccessRightsElement & element)
{
    assert(!element.is_partial_revoke);
    assert(!element.grant_option || with_grant_option);

    if (element.isGlobalWithParameter())
    {
        if (element.anyParameter())
            grantImpl<with_grant_option, wildcard>(element.access_flags);
        else
            grantImpl<with_grant_option, wildcard>(element.access_flags, element.parameter);
    }
    else if (element.anyDatabase())
        grantImpl<with_grant_option, wildcard>(element.access_flags);
    else if (element.anyTable())
        grantImpl<with_grant_option, wildcard>(element.access_flags, element.database);
    else if (element.anyColumn())
        grantImpl<with_grant_option, wildcard>(element.access_flags, element.database, element.table);
    else
        grantImpl<with_grant_option, wildcard>(element.access_flags, element.database, element.table, element.columns);
}

template <bool with_grant_option, bool wildcard>
void AccessRights::grantImpl(const AccessRightsElement & element)
{
    if (element.is_partial_revoke)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A partial revoke should be revoked, not granted");
    if (element.wildcard)
    {
        if (element.grant_option)
            grantImplHelper<true, true>(element);
        else
            grantImplHelper<with_grant_option, true>(element);
    }
    else
    {
        if (element.grant_option)
            grantImplHelper<true, wildcard>(element);
        else
            grantImplHelper<with_grant_option, wildcard>(element);
    }
}

template <bool with_grant_option, bool wildcard>
void AccessRights::grantImpl(const AccessRightsElements & elements)
{
    for (const auto & element : elements)
        grantImpl<with_grant_option, wildcard>(element);
}

void AccessRights::grant(const AccessFlags & flags) { grantImpl<false, false>(flags); }
void AccessRights::grant(const AccessFlags & flags, std::string_view database) { grantImpl<false, false>(flags, database); }
void AccessRights::grant(const AccessFlags & flags, std::string_view database, std::string_view table) { grantImpl<false, false>(flags, database, table); }
void AccessRights::grant(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) { grantImpl<false, false>(flags, database, table, column); }
void AccessRights::grant(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) { grantImpl<false, false>(flags, database, table, columns); }
void AccessRights::grant(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) { grantImpl<false, false>(flags, database, table, columns); }
void AccessRights::grant(const AccessRightsElement & element) { grantImpl<false, false>(element); }
void AccessRights::grant(const AccessRightsElements & elements) { grantImpl<false, false>(elements); }

void AccessRights::grantWildcard(const AccessFlags & flags) { grantImpl<false, true>(flags); }
void AccessRights::grantWildcard(const AccessFlags & flags, std::string_view database) { grantImpl<false, true>(flags, database); }
void AccessRights::grantWildcard(const AccessFlags & flags, std::string_view database, std::string_view table) { grantImpl<false, true>(flags, database, table); }
void AccessRights::grantWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) { grantImpl<false, true>(flags, database, table, column); }
void AccessRights::grantWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) { grantImpl<false, true>(flags, database, table, columns); }
void AccessRights::grantWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) { grantImpl<false, true>(flags, database, table, columns); }

void AccessRights::grantWithGrantOption(const AccessFlags & flags) { grantImpl<true, false>(flags); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, std::string_view database) { grantImpl<true, false>(flags, database); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table) { grantImpl<true, false>(flags, database, table); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) { grantImpl<true, false>(flags, database, table, column); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) { grantImpl<true, false>(flags, database, table, columns); }
void AccessRights::grantWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) { grantImpl<true, false>(flags, database, table, columns); }
void AccessRights::grantWithGrantOption(const AccessRightsElement & element) { grantImpl<true, false>(element); }
void AccessRights::grantWithGrantOption(const AccessRightsElements & elements) { grantImpl<true, false>(elements); }

void AccessRights::grantWildcardWithGrantOption(const AccessFlags & flags) { grantImpl<true, true>(flags); }
void AccessRights::grantWildcardWithGrantOption(const AccessFlags & flags, std::string_view database) { grantImpl<true, true>(flags, database); }
void AccessRights::grantWildcardWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table) { grantImpl<true, true>(flags, database, table); }
void AccessRights::grantWildcardWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) { grantImpl<true, true>(flags, database, table, column); }
void AccessRights::grantWildcardWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) { grantImpl<true, true>(flags, database, table, columns); }
void AccessRights::grantWildcardWithGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) { grantImpl<true, true>(flags, database, table, columns); }


template <bool grant_option, bool wildcard, typename... Args>
void AccessRights::revokeImpl(const AccessFlags & flags, const Args &... args)
{
    auto helper = [&](std::unique_ptr<Node> & root_node)
    {
        if (!root_node)
            return;
        root_node->revoke<wildcard>(flags, args...);
        if (!root_node->flags && !root_node->children)
            root_node = nullptr;
    };
    helper(root_with_grant_option);

    if constexpr (!grant_option)
        helper(root);
}

template <bool grant_option, bool wildcard>
void AccessRights::revokeImplHelper(const AccessRightsElement & element)
{
    assert(!element.grant_option || grant_option);
    if (element.isGlobalWithParameter())
    {
        if (element.anyParameter())
            revokeImpl<grant_option, wildcard>(element.access_flags);
        else
            revokeImpl<grant_option, wildcard>(element.access_flags, element.parameter);
    }
    else if (element.anyDatabase())
        revokeImpl<grant_option, wildcard>(element.access_flags);
    else if (element.anyTable())
        revokeImpl<grant_option, wildcard>(element.access_flags, element.database);
    else if (element.anyColumn())
        revokeImpl<grant_option, wildcard>(element.access_flags, element.database, element.table);
    else
        revokeImpl<grant_option, wildcard>(element.access_flags, element.database, element.table, element.columns);
}

template <bool grant_option, bool wildcard>
void AccessRights::revokeImpl(const AccessRightsElement & element)
{
    if (element.wildcard)
    {
        if (element.grant_option)
            revokeImplHelper<true, true>(element);
        else
            revokeImplHelper<grant_option, true>(element);
    }
    else
    {
        if (element.grant_option)
            revokeImplHelper<true, wildcard>(element);
        else
            revokeImplHelper<grant_option, wildcard>(element);
    }
}

template <bool grant_option, bool wildcard>
void AccessRights::revokeImpl(const AccessRightsElements & elements)
{
    for (const auto & element : elements)
        revokeImpl<grant_option, wildcard>(element);
}

void AccessRights::revoke(const AccessFlags & flags) { revokeImpl<false, false>(flags); }
void AccessRights::revoke(const AccessFlags & flags, std::string_view database) { revokeImpl<false, false>(flags, database); }
void AccessRights::revoke(const AccessFlags & flags, std::string_view database, std::string_view table) { revokeImpl<false, false>(flags, database, table); }
void AccessRights::revoke(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) { revokeImpl<false, false>(flags, database, table, column); }
void AccessRights::revoke(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) { revokeImpl<false, false>(flags, database, table, columns); }
void AccessRights::revoke(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) { revokeImpl<false, false>(flags, database, table, columns); }
void AccessRights::revoke(const AccessRightsElement & element) { revokeImpl<false, false>(element); }
void AccessRights::revoke(const AccessRightsElements & elements) { revokeImpl<false, false>(elements); }

void AccessRights::revokeWildcard(const AccessFlags & flags) { revokeImpl<false, true>(flags); }
void AccessRights::revokeWildcard(const AccessFlags & flags, std::string_view database) { revokeImpl<false, true>(flags, database); }
void AccessRights::revokeWildcard(const AccessFlags & flags, std::string_view database, std::string_view table) { revokeImpl<false, true>(flags, database, table); }
void AccessRights::revokeWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) { revokeImpl<false, true>(flags, database, table, column); }
void AccessRights::revokeWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) { revokeImpl<false, true>(flags, database, table, columns); }
void AccessRights::revokeWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) { revokeImpl<false, true>(flags, database, table, columns); }

void AccessRights::revokeGrantOption(const AccessFlags & flags) { revokeImpl<true, false>(flags); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, std::string_view database) { revokeImpl<true, false>(flags, database); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table) { revokeImpl<true, false>(flags, database, table); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) { revokeImpl<true, false>(flags, database, table, column); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) { revokeImpl<true, false>(flags, database, table, columns); }
void AccessRights::revokeGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) { revokeImpl<true, false>(flags, database, table, columns); }
void AccessRights::revokeGrantOption(const AccessRightsElement & element) { revokeImpl<true, false>(element); }
void AccessRights::revokeGrantOption(const AccessRightsElements & elements) { revokeImpl<true, false>(elements); }

void AccessRights::revokeWildcardGrantOption(const AccessFlags & flags) { revokeImpl<true, true>(flags); }
void AccessRights::revokeWildcardGrantOption(const AccessFlags & flags, std::string_view database) { revokeImpl<true, true>(flags, database); }
void AccessRights::revokeWildcardGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table) { revokeImpl<true, true>(flags, database, table); }
void AccessRights::revokeWildcardGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) { revokeImpl<true, true>(flags, database, table, column); }
void AccessRights::revokeWildcardGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) { revokeImpl<true, true>(flags, database, table, columns); }
void AccessRights::revokeWildcardGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) { revokeImpl<true, true>(flags, database, table, columns); }

AccessRightsElements AccessRights::getElements() const
{
    if (!root)
        return {};
    if (!root_with_grant_option)
        return root->getElements().getResult();
    return Node::getElements(*root, *root_with_grant_option).getResult();
}


String AccessRights::toString() const
{
    return getElements().toString();
}


template <bool grant_option, bool wildcard, typename... Args>
bool AccessRights::isGrantedImpl(const AccessFlags & flags, const Args &... args) const
{
    auto helper = [&](const std::unique_ptr<Node> & root_node) -> bool
    {
        if (!root_node)
            return flags.isEmpty();
        return root_node->isGranted<wildcard>(flags, args...);
    };
    if constexpr (grant_option)
        return helper(root_with_grant_option);
    else
        return helper(root);
}

template <bool grant_option>
bool AccessRights::containsImpl(const AccessRights & other) const
{
    auto helper = [&](const std::unique_ptr<Node> & root_node) -> bool
    {
        if (!root_node)
            return !other.root;
        if (!other.root)
            return true;
        return root_node->contains(*other.root);
    };
    if constexpr (grant_option)
        return helper(root_with_grant_option);
    else
        return helper(root);
}


template <bool grant_option, bool wildcard>
bool AccessRights::isGrantedImplHelper(const AccessRightsElement & element) const
{
    assert(!element.grant_option || grant_option);
    if (element.isGlobalWithParameter())
    {
        if (element.anyParameter())
            return isGrantedImpl<grant_option, wildcard>(element.access_flags);

        return isGrantedImpl<grant_option, wildcard>(element.access_flags, element.parameter);
    }
    if (element.anyDatabase())
        return isGrantedImpl<grant_option, wildcard>(element.access_flags);
    if (element.anyTable())
        return isGrantedImpl<grant_option, wildcard>(element.access_flags, element.database);
    if (element.anyColumn())
        return isGrantedImpl<grant_option, wildcard>(element.access_flags, element.database, element.table);

    return isGrantedImpl<grant_option, wildcard>(element.access_flags, element.database, element.table, element.columns);
}

template <bool grant_option, bool wildcard>
bool AccessRights::isGrantedImpl(const AccessRightsElement & element) const
{
    if (element.wildcard)
    {
        if (element.grant_option)
            return isGrantedImplHelper<true, true>(element);

        return isGrantedImplHelper<grant_option, true>(element);
    }

    if (element.grant_option)
        return isGrantedImplHelper<true, wildcard>(element);

    return isGrantedImplHelper<grant_option, wildcard>(element);
}

template <bool grant_option, bool wildcard>
bool AccessRights::isGrantedImpl(const AccessRightsElements & elements) const
{
    for (const auto & element : elements)
        if (!isGrantedImpl<grant_option, wildcard>(element))
            return false;
    return true;
}

bool AccessRights::isGranted(const AccessFlags & flags) const { return isGrantedImpl<false, false>(flags); }
bool AccessRights::isGranted(const AccessFlags & flags, std::string_view database) const { return isGrantedImpl<false, false>(flags, database); }
bool AccessRights::isGranted(const AccessFlags & flags, std::string_view database, std::string_view table) const { return isGrantedImpl<false, false>(flags, database, table); }
bool AccessRights::isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { return isGrantedImpl<false, false>(flags, database, table, column); }
bool AccessRights::isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { return isGrantedImpl<false, false>(flags, database, table, columns); }
bool AccessRights::isGranted(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { return isGrantedImpl<false, false>(flags, database, table, columns); }
bool AccessRights::isGranted(const AccessRightsElement & element) const { return isGrantedImpl<false, false>(element); }
bool AccessRights::isGranted(const AccessRightsElements & elements) const { return isGrantedImpl<false, false>(elements); }

bool AccessRights::isGrantedWildcard(const AccessFlags & flags) const { return isGrantedImpl<false, true>(flags); }
bool AccessRights::isGrantedWildcard(const AccessFlags & flags, std::string_view database) const { return isGrantedImpl<false, true>(flags, database); }
bool AccessRights::isGrantedWildcard(const AccessFlags & flags, std::string_view database, std::string_view table) const { return isGrantedImpl<false, true>(flags, database, table); }
bool AccessRights::isGrantedWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { return isGrantedImpl<false, true>(flags, database, table, column); }
bool AccessRights::isGrantedWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { return isGrantedImpl<false, true>(flags, database, table, columns); }
bool AccessRights::isGrantedWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { return isGrantedImpl<false, true>(flags, database, table, columns); }

bool AccessRights::hasGrantOption(const AccessFlags & flags) const { return isGrantedImpl<true, false>(flags); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, std::string_view database) const { return isGrantedImpl<true, false>(flags, database); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table) const { return isGrantedImpl<true, false>(flags, database, table); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { return isGrantedImpl<true, false>(flags, database, table, column); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { return isGrantedImpl<true, false>(flags, database, table, columns); }
bool AccessRights::hasGrantOption(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { return isGrantedImpl<true, false>(flags, database, table, columns); }
bool AccessRights::hasGrantOption(const AccessRightsElement & element) const { return isGrantedImpl<true, false>(element); }
bool AccessRights::hasGrantOption(const AccessRightsElements & elements) const { return isGrantedImpl<true, false>(elements); }

bool AccessRights::hasGrantOptionWildcard(const AccessFlags & flags) const { return isGrantedImpl<true, true>(flags); }
bool AccessRights::hasGrantOptionWildcard(const AccessFlags & flags, std::string_view database) const { return isGrantedImpl<true, true>(flags, database); }
bool AccessRights::hasGrantOptionWildcard(const AccessFlags & flags, std::string_view database, std::string_view table) const { return isGrantedImpl<true, true>(flags, database, table); }
bool AccessRights::hasGrantOptionWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { return isGrantedImpl<true, true>(flags, database, table, column); }
bool AccessRights::hasGrantOptionWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { return isGrantedImpl<true, true>(flags, database, table, columns); }
bool AccessRights::hasGrantOptionWildcard(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { return isGrantedImpl<true, true>(flags, database, table, columns); }

bool AccessRights::contains(const AccessRights & access_rights) const { return containsImpl<false>(access_rights); }
bool AccessRights::containsWithGrantOption(const AccessRights & access_rights) const { return containsImpl<true>(access_rights); }

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


void AccessRights::makeDifference(const AccessRights & other)
{
    auto helper = [](std::unique_ptr<Node> & root_node, const std::unique_ptr<Node> & other_root_node)
    {
        if (!root_node || !other_root_node)
            return;

        root_node->makeDifference(*other_root_node);
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
    root->modifyFlags(function, false, flags_added, flags_removed);
    if (flags_removed && root_with_grant_option)
        root_with_grant_option->makeIntersection(*root);

    if (root_with_grant_option)
    {
        root_with_grant_option->modifyFlags(function, true, flags_added, flags_removed);
        if (flags_added)
        {
            if (!root)
                root = std::make_unique<Node>();
            root->makeUnion(*root_with_grant_option);
        }
    }
}


AccessRights AccessRights::getFullAccess()
{
    AccessRights res;
    res.grantWithGrantOption(AccessType::ALL);
    return res;
}


void AccessRights::dumpTree(WriteBuffer & buffer) const
{
    if (root)
    {
        root->dumpTree(buffer, "");
        if (root_with_grant_option)
            root->dumpTree(buffer, "go");
    }
}

std::vector<String> AccessRights::dumpNodes() const
{
    std::vector<String> result;
    for (auto it = root->begin(); it != root->end(); ++it)
        result.push_back(it.getPath());

    return result;
}


}
