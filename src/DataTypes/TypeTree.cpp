#include <DataTypes/TypeTree.h>

#include <DataTypes/DataTypeCustom.h>
#include <Common/checkStackSize.h>


namespace DB
{

void forEachInTypeTree(const IDataType & type, const std::function<void(const IDataType &)> & callback)
{
    checkStackSize();

    callback(type);
    for (const auto & child : type.getChildren())
        forEachInTypeTree(*child, callback);
}

bool anyInTypeTree(const IDataType & type, const std::function<bool(const IDataType &)> & predicate)
{
    checkStackSize();

    if (predicate(type))
        return true;

    for (const auto & child : type.getChildren())
        if (anyInTypeTree(*child, predicate))
            return true;

    return false;
}

DataTypePtr rewriteTypeTree(const DataTypePtr & type, const TypeTreeRewriteFn & callback, CustomizationPolicy policy)
{
    checkStackSize();

    const DataTypes children = type->getChildren();

    DataTypes new_children;
    new_children.reserve(children.size());
    bool any_child_moved = false;
    for (const auto & child : children)
    {
        auto new_child = rewriteTypeTree(child, callback, policy);
        if (!new_child)
            return nullptr;

        any_child_moved |= new_child.get() != child.get();
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
                rederived = custom_name->rederiveFor(rebuilt);

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
