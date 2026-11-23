#include <DataTypes/TypeTree.h>

#include <DataTypes/DataTypeCustom.h>
#include <Common/checkStackSize.h>


namespace DB
{

void forEachInTypeTree(const IDataType & type, const std::function<void(const IDataType &)> & callback)
{
    checkStackSize();

    callback(type);
    const size_t num_children = type.getNumberOfChildren();
    for (size_t i = 0; i < num_children; ++i)
        forEachInTypeTree(*type.getChild(i), callback);
}

bool anyInTypeTree(const IDataType & type, const std::function<bool(const IDataType &)> & predicate)
{
    checkStackSize();

    if (predicate(type))
        return true;

    const size_t num_children = type.getNumberOfChildren();
    for (size_t i = 0; i < num_children; ++i)
        if (anyInTypeTree(*type.getChild(i), predicate))
            return true;

    return false;
}

DataTypePtr rewriteTypeTree(const DataTypePtr & type, const TypeTreeRewriteFn & callback, CustomizationPolicy policy)
{
    checkStackSize();

    const size_t num_children = type->getNumberOfChildren();

    /// Most rewrites leave most subtrees alone - `setVersionToAggregateFunctions` runs on every column
    /// of every block a `NativeWriter` sends, and touches only the aggregate states among them - so the
    /// replacement list is only materialized once a child has actually moved.
    DataTypes new_children;
    bool any_child_moved = false;
    for (size_t i = 0; i < num_children; ++i)
    {
        const DataTypePtr & child = type->getChild(i);
        auto new_child = rewriteTypeTree(child, callback, policy);
        if (!new_child)
            return nullptr;

        if (!any_child_moved && new_child.get() != child.get())
        {
            any_child_moved = true;
            new_children.reserve(num_children);
            for (size_t j = 0; j < i; ++j)
                new_children.push_back(type->getChild(j));
        }

        if (any_child_moved)
            new_children.push_back(std::move(new_child));
    }

    DataTypePtr rebuilt = type;
    if (any_child_moved)
    {
        rebuilt = type->cloneWithChildren(new_children);

        /// `cloneWithChildren` builds the plain type, so a customization has to be re-derived for the
        /// new children explicitly rather than carried over by construction.
        if (type->hasCustomName() || type->getCustomSerialization())
        {
            /// A customization can only be re-derived through its custom name. One that carries a
            /// serialization and no name - the `Quantized` codec attaches such a customization - has
            /// nothing to ask, and can only be preserved by keeping the whole subtree.
            DataTypeCustomDescPtr rederived;
            if (const auto * custom_name = type->getCustomName())
                rederived = custom_name->rederiveFor(
                    rebuilt, [&](const DataTypePtr & nested) { return rewriteTypeTree(nested, callback, policy); });

            /// A re-derivation that drops the custom serialization would have the column read back with
            /// a different one, so it does not count as having followed the rewrite either.
            const bool followed = rederived && (!type->getCustomSerialization() || rederived->serialization);

            if (followed)
                rebuilt->setCustomization(std::move(rederived));
            else if (policy == CustomizationPolicy::Refuse)
                return nullptr;
            else
                rebuilt = type;
        }
    }

    return callback(rebuilt);
}

}
