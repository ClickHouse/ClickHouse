#include <DataTypes/transformTypesRecursively.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeVariant.h>
#include <Formats/SchemaInferenceUtils.h>

#include <numeric>
#include <optional>
#include <unordered_set>


namespace DB
{

namespace
{

TypeIndexesSet getTypesIndexes(const DataTypes & types)
{
    TypeIndexesSet type_indexes;
    for (const auto & type : types)
        type_indexes.insert(type->getTypeId());
    return type_indexes;
}

bool isComplexType(TypeIndex index)
{
    switch (index)
    {
        case TypeIndex::Nullable:
        case TypeIndex::LowCardinality:
        case TypeIndex::Array:
        case TypeIndex::Map:
        case TypeIndex::Tuple:
        case TypeIndex::Object:
        case TypeIndex::Variant:
            return true;
        default:
            return false;
    }
}

bool hasEqualsAmbiguousAlternatives(const DataTypes & alternatives)
{
    for (size_t i = 0; i + 1 < alternatives.size(); ++i)
        for (size_t j = i + 1; j < alternatives.size(); ++j)
            if (alternatives[i]->equals(*alternatives[j]))
                return true;
    return false;
}

/// For the types of one complex kind, the index of the child of each type paired with each child of the first one,
/// or nothing when their children do not line up.
std::optional<std::vector<std::vector<size_t>>> pairChildren(const DataTypes & types, TypeIndex kind)
{
    const size_t num_children = types[0]->getNumberOfChildren();
    std::vector<std::vector<size_t>> positions(types.size(), std::vector<size_t>(num_children));
    for (size_t i = 0; i != types.size(); ++i)
    {
        if (types[i]->getNumberOfChildren() != num_children)
            return {};
        std::iota(positions[i].begin(), positions[i].end(), 0);
    }

    if (kind == TypeIndex::Tuple)
    {
        const auto & first = assert_cast<const DataTypeTuple &>(*types[0]);
        for (const auto & type : types)
        {
            const auto & tuple = assert_cast<const DataTypeTuple &>(*type);
            if (tuple.hasExplicitNames() != first.hasExplicitNames() || tuple.getElementNames() != first.getElementNames())
                return {};
        }
    }
    else if (kind == TypeIndex::Object)
    {
        /// The children are the typed paths in the order of their names, so the same set of paths pairs them by path.
        const auto & first_paths = assert_cast<const DataTypeObject &>(*types[0]).getTypedPaths();
        for (const auto & type : types)
            for (const auto & [path, _] : assert_cast<const DataTypeObject &>(*type).getTypedPaths())
                if (!first_paths.contains(path))
                    return {};
    }
    else if (kind == TypeIndex::Variant)
    {
        /// The position of an alternative is the order of the names, which `equals` does not compare.
        for (size_t i = 0; i != types.size(); ++i)
        {
            const auto & alternatives = assert_cast<const DataTypeVariant &>(*types[i]).getVariants();
            if (hasEqualsAmbiguousAlternatives(alternatives))
                return {};
            for (size_t child = 0; child != num_children; ++child)
            {
                auto it = std::ranges::find_if(
                    alternatives, [&](const DataTypePtr & alternative) { return alternative->equals(*types[0]->getChild(child)); });
                if (it == alternatives.end())
                    return {};
                positions[i][child] = it - alternatives.begin();
            }
        }
    }

    return positions;
}

}

void transformTypesRecursively(
    DataTypes & types,
    std::function<void(DataTypes &, TypeIndexesSet &)> transform_simple_types,
    std::function<void(DataTypes &, TypeIndexesSet &)> transform_complex_types,
    const FormatSettings * format_settings,
    const TypeIndexesSet & types_to_not_descend)
{
    TypeIndexesSet type_indexes = getTypesIndexes(types);

    for (const auto wrapper : {TypeIndex::Nullable, TypeIndex::LowCardinality})
    {
        if (!type_indexes.contains(wrapper) || types_to_not_descend.contains(wrapper))
            continue;

        DataTypes nested_types;
        nested_types.reserve(types.size());
        for (const auto & type : types)
            nested_types.push_back(type->getTypeId() == wrapper ? type->getChild(0) : type);

        transformTypesRecursively(nested_types, transform_simple_types, transform_complex_types, format_settings, types_to_not_descend);
        for (size_t i = 0; i != types.size(); ++i)
        {
            const auto & nested = nested_types[i];
            if (types[i]->getTypeId() != wrapper)
            {
                types[i] = nested;
                continue;
            }

            /// Type could be changed so it cannot be inside the wrapper anymore.
            bool can_be_wrapped = nested->canBeInsideLowCardinality();
            if (wrapper == TypeIndex::Nullable)
                can_be_wrapped = format_settings ? canBeInsideNullableBySchemaSettings(nested, *format_settings) : nested->canBeInsideNullable();

            if (!can_be_wrapped)
                types[i] = nested;
            else if (nested.get() != types[i]->getChild(0).get())
                types[i] = types[i]->cloneWithChildren({nested});
        }

        if (transform_complex_types)
        {
            /// Some types could be changed.
            type_indexes = getTypesIndexes(types);
            transform_complex_types(types, type_indexes);
        }

        return;
    }

    if (std::ranges::none_of(type_indexes, isComplexType))
    {
        transform_simple_types(types, type_indexes);
        return;
    }

    const TypeIndex kind = *type_indexes.begin();
    std::optional<std::vector<std::vector<size_t>>> positions;
    if (type_indexes.size() == 1 && !types_to_not_descend.contains(kind))
        positions = pairChildren(types, kind);

    if (positions)
    {
        std::vector<DataTypes> children(types.size());
        for (size_t i = 0; i != types.size(); ++i)
            children[i] = types[i]->getChildren();

        DataTypes paired(types.size());
        for (size_t child = 0; child != children[0].size(); ++child)
        {
            for (size_t i = 0; i != types.size(); ++i)
                paired[i] = children[i][(*positions)[i][child]];
            transformTypesRecursively(paired, transform_simple_types, transform_complex_types, format_settings, types_to_not_descend);
            for (size_t i = 0; i != types.size(); ++i)
                children[i][(*positions)[i][child]] = std::move(paired[i]);
        }

        for (size_t i = 0; i != types.size(); ++i)
        {
            for (size_t child = 0; child != children[i].size(); ++child)
            {
                if (children[i][child].get() != types[i]->getChild(child).get())
                {
                    types[i] = types[i]->cloneWithChildren(children[i]);
                    break;
                }
            }
        }
    }

    if (transform_complex_types)
        transform_complex_types(types, type_indexes);
}

namespace
{

DataTypePtr replaceNestedTypesInPairImpl(
    const DataTypePtr & left,
    const DataTypePtr & right,
    const PairedLeafCallback & on_leaf,
    bool skip_custom_name,
    bool left_is_common_type)
{
    const WhichDataType left_which(left);
    const WhichDataType right_which(right);

    /// Neither wrapper hides a leaf, so a pair differing only by one of them still has to be walked.
    if (right_which.isLowCardinality() && !left_which.isLowCardinality())
        return replaceNestedTypesInPairImpl(left, removeLowCardinality(right), on_leaf, skip_custom_name, left_is_common_type);
    if (right_which.isNullable() && !left_which.isNullable())
        return replaceNestedTypesInPairImpl(left, removeNullable(right), on_leaf, skip_custom_name, left_is_common_type);

    const bool descendable = left_which.isNullable() || left_which.isLowCardinality() || left_which.isArray()
        || left_which.isMap() || left_which.isTuple() || left_which.isVariant() || (left_is_common_type && left_which.isObject());

    if (!descendable)
        return on_leaf(left, right);

    if (left_is_common_type && right_which.isNothing())
        return left;

    /// Rebuilding below cannot keep the name, so hand back `right` whole, and only once the children clear:
    /// restoring a customization over children that did not is how a relabel changes what a level means.
    if (!skip_custom_name && (left->hasCustomName() || (right->hasCustomName() && !left_is_common_type))
        && left->getName() != right->getName())
    {
        if (!left->equals(*right))
            return nullptr;
        return replaceNestedTypesInPairImpl(left, right, on_leaf, /*skip_custom_name=*/ true, left_is_common_type) ? right : nullptr;
    }

    if (left_which.isNullable())
    {
        const DataTypePtr left_nested = removeNullable(left);
        auto nested = replaceNestedTypesInPairImpl(left_nested, removeNullable(right), on_leaf, false, left_is_common_type);
        if (!nested)
            return nullptr;
        return nested.get() == left_nested.get() ? left : makeNullable(nested);
    }

    if (left_which.isLowCardinality())
    {
        const DataTypePtr left_nested = removeLowCardinality(left);
        auto nested = replaceNestedTypesInPairImpl(left_nested, removeLowCardinality(right), on_leaf, false, left_is_common_type);
        if (!nested)
            return nullptr;
        return nested.get() == left_nested.get() ? left : std::make_shared<DataTypeLowCardinality>(nested);
    }

    if (left_which.isArray())
    {
        const auto * right_array = typeid_cast<const DataTypeArray *>(right.get());
        if (!right_array)
            return nullptr;
        const DataTypePtr left_nested = assert_cast<const DataTypeArray &>(*left).getNestedType();
        auto nested = replaceNestedTypesInPairImpl(left_nested, right_array->getNestedType(), on_leaf, false, left_is_common_type);
        if (!nested)
            return nullptr;
        return nested.get() == left_nested.get() ? left : std::make_shared<DataTypeArray>(nested);
    }

    if (left_which.isMap())
    {
        const auto * right_map = typeid_cast<const DataTypeMap *>(right.get());
        if (!right_map)
            return nullptr;
        const auto & left_map = assert_cast<const DataTypeMap &>(*left);
        auto key = replaceNestedTypesInPairImpl(left_map.getKeyType(), right_map->getKeyType(), on_leaf, false, left_is_common_type);
        auto value = replaceNestedTypesInPairImpl(left_map.getValueType(), right_map->getValueType(), on_leaf, false, left_is_common_type);
        if (!key || !value)
            return nullptr;
        if (key.get() == left_map.getKeyType().get() && value.get() == left_map.getValueType().get())
            return left;
        return std::make_shared<DataTypeMap>(key, value);
    }

    if (left_which.isTuple())
    {
        const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right.get());
        const auto & left_tuple = assert_cast<const DataTypeTuple &>(*left);
        if (!right_tuple || right_tuple->getElements().size() != left_tuple.getElements().size())
            return nullptr;
        /// `DataTypeTuple::equals` cannot see the naming mode (an anonymous tuple compares by its ordinal
        /// names), while serialization does (`toJSONString` writes an object or an array), so a pair that
        /// disagrees on it is not interchangeable whether or not a child moves.
        if (!left_is_common_type && right_tuple->hasExplicitNames() != left_tuple.hasExplicitNames())
            return nullptr;
        DataTypes elements;
        elements.reserve(left_tuple.getElements().size());
        bool moved = false;
        for (size_t i = 0; i < left_tuple.getElements().size(); ++i)
        {
            auto nested = replaceNestedTypesInPairImpl(
                left_tuple.getElements()[i], right_tuple->getElements()[i], on_leaf, false, left_is_common_type);
            if (!nested)
                return nullptr;
            moved |= nested.get() != left_tuple.getElements()[i].get();
            elements.push_back(std::move(nested));
        }
        if (!moved)
            return left;
        if (left_tuple.hasExplicitNames())
            return std::make_shared<DataTypeTuple>(elements, left_tuple.getElementNames());
        return std::make_shared<DataTypeTuple>(elements);
    }

    if (left_which.isObject())
    {
        const auto * right_object = typeid_cast<const DataTypeObject *>(right.get());
        if (!right_object)
            return nullptr;
        const auto & left_object = assert_cast<const DataTypeObject &>(*left);
        DataTypeObject::TypedPathsMap typed_paths;
        bool moved = false;
        for (const auto & [path, type] : left_object.getTypedPaths())
        {
            auto it = right_object->getTypedPaths().find(path);
            if (it == right_object->getTypedPaths().end())
                return nullptr;
            auto nested = replaceNestedTypesInPairImpl(type, it->second, on_leaf, false, left_is_common_type);
            if (!nested)
                return nullptr;
            moved |= nested.get() != type.get();
            typed_paths.emplace(path, std::move(nested));
        }
        if (!moved)
            return left;
        return std::make_shared<DataTypeObject>(
            left_object.getSchemaFormat(),
            std::move(typed_paths),
            left_object.getPathsToSkip(),
            left_object.getPathRegexpsToSkip(),
            left_object.getMaxDynamicPaths(),
            left_object.getMaxDynamicTypes(),
            left_object.getSharedTypedPaths());
    }

    const auto * right_variant = typeid_cast<const DataTypeVariant *>(right.get());
    const auto & left_variant = assert_cast<const DataTypeVariant &>(*left);
    if (!right_variant || right_variant->getVariants().size() != left_variant.getVariants().size())
        return nullptr;
    DataTypes alternatives;
    alternatives.reserve(left_variant.getVariants().size());
    std::unordered_set<String> names;
    bool moved = false;
    for (size_t i = 0; i < left_variant.getVariants().size(); ++i)
    {
        auto nested = replaceNestedTypesInPairImpl(
            left_variant.getVariants()[i], right_variant->getVariants()[i], on_leaf, false, left_is_common_type);
        if (!nested)
            return nullptr;
        /// A Variant holding one type twice has no unambiguous discriminator for it, and a replacement can
        /// turn two alternatives that differed only in a name into the same type.
        if (!names.insert(nested->getName()).second)
            return nullptr;
        moved |= nested.get() != left_variant.getVariants()[i].get();
        alternatives.push_back(std::move(nested));
    }
    if (!moved)
        return left;
    /// Alternatives are paired by position, which is what `DataTypeVariant::equals` compares, but position
    /// is only a canonical name order: once a side holds two alternatives that are `equals`-equal to each
    /// other, several pairings satisfy `equals` and a replacement would announce one alternative's values
    /// under another's. `allow_suspicious_variant_types` is what admits such a type in the first place.
    if (hasEqualsAmbiguousAlternatives(left_variant.getVariants())
        || hasEqualsAmbiguousAlternatives(right_variant->getVariants()))
        return nullptr;
    /// The canonical constructor sorts alternatives by name and squashes equal ones, either of which would
    /// renumber the discriminators the data was written with.
    return std::make_shared<DataTypeVariant>(alternatives, DataTypeVariant::FixedDiscriminatorOrder{});
}

}

DataTypePtr replaceNestedTypesInPair(
    const DataTypePtr & left, const DataTypePtr & right, const PairedLeafCallback & on_leaf, bool left_is_common_type)
{
    return replaceNestedTypesInPairImpl(left, right, on_leaf, /*skip_custom_name=*/ false, left_is_common_type);
}

}
