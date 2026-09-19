#include <DataTypes/transformTypesRecursively.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <Formats/SchemaInferenceUtils.h>
#include <Common/Exception.h>

#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static TypeIndexesSet getTypesIndexes(const DataTypes & types)
{
    TypeIndexesSet type_indexes;
    for (const auto & type : types)
        type_indexes.insert(type->getTypeId());
    return type_indexes;
}

void transformTypesRecursively(
    DataTypes & types,
    std::function<void(DataTypes &, TypeIndexesSet &)> transform_simple_types,
    std::function<void(DataTypes &, TypeIndexesSet &)> transform_complex_types,
    const FormatSettings * format_settings)
{
    TypeIndexesSet type_indexes = getTypesIndexes(types);

    /// Nullable
    if (type_indexes.contains(TypeIndex::Nullable))
    {
        std::vector<UInt8> is_nullable;
        is_nullable.reserve(types.size());
        DataTypes nested_types;
        nested_types.reserve(types.size());
        for (const auto & type : types)
        {
            if (const DataTypeNullable * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
            {
                is_nullable.push_back(1);
                nested_types.push_back(type_nullable->getNestedType());
            }
            else
            {
                is_nullable.push_back(0);
                nested_types.push_back(type);
            }
        }

        transformTypesRecursively(nested_types, transform_simple_types, transform_complex_types, format_settings);
        for (size_t i = 0; i != types.size(); ++i)
        {
            /// Type could be changed so it cannot be inside Nullable anymore.
            const bool can_make_nullable = format_settings ? canBeInsideNullableBySchemaSettings(nested_types[i], *format_settings)
                                                           : nested_types[i]->canBeInsideNullable();
            if (is_nullable[i] && can_make_nullable)
                types[i] = makeNullable(nested_types[i]);
            else
                types[i] = nested_types[i];
        }

        if (transform_complex_types)
        {
            /// Some types could be changed.
            type_indexes = getTypesIndexes(types);
            transform_complex_types(types, type_indexes);
        }

        return;
    }

    /// Arrays
    if (type_indexes.contains(TypeIndex::Array))
    {
        /// All types are Array
        if (type_indexes.size() == 1)
        {
            DataTypes nested_types;
            for (const auto & type : types)
                nested_types.push_back(typeid_cast<const DataTypeArray *>(type.get())->getNestedType());

            transformTypesRecursively(nested_types, transform_simple_types, transform_complex_types, format_settings);
            for (size_t i = 0; i != types.size(); ++i)
                types[i] = std::make_shared<DataTypeArray>(nested_types[i]);
        }

        if (transform_complex_types)
            transform_complex_types(types, type_indexes);

        return;
    }

    /// Tuples
    if (type_indexes.contains(TypeIndex::Tuple))
    {
        /// All types are Tuple
        if (type_indexes.size() == 1)
        {
            std::vector<DataTypes> nested_types;
            const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(types[0].get());
            size_t tuple_size = type_tuple->getElements().size();
            bool has_explicit_names = type_tuple->hasExplicitNames();
            nested_types.resize(tuple_size);
            for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                nested_types[elem_idx].reserve(types.size());

            /// Apply transform to elements only if all tuples are the same.
            bool sizes_are_equal = true;
            Names element_names = type_tuple->getElementNames();
            bool all_element_names_are_equal = true;
            for (const auto & type : types)
            {
                type_tuple = typeid_cast<const DataTypeTuple *>(type.get());
                if (type_tuple->getElements().size() != tuple_size)
                {
                    sizes_are_equal = false;
                    break;
                }

                if (type_tuple->getElementNames() != element_names)
                {
                    all_element_names_are_equal = false;
                    break;
                }

                for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                    nested_types[elem_idx].emplace_back(type_tuple->getElements()[elem_idx]);
            }

            if (sizes_are_equal && all_element_names_are_equal)
            {
                std::vector<DataTypes> transposed_nested_types(types.size());
                for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                {
                    transformTypesRecursively(nested_types[elem_idx], transform_simple_types, transform_complex_types, format_settings);
                    for (size_t i = 0; i != types.size(); ++i)
                        transposed_nested_types[i].push_back(nested_types[elem_idx][i]);
                }

                for (size_t i = 0; i != types.size(); ++i)
                {
                    if (has_explicit_names)
                        types[i] = std::make_shared<DataTypeTuple>(transposed_nested_types[i], element_names);
                    else
                        types[i] = std::make_shared<DataTypeTuple>(transposed_nested_types[i]);
                }
            }
        }

        if (transform_complex_types)
            transform_complex_types(types, type_indexes);

        return;
    }

    /// Maps
    if (type_indexes.contains(TypeIndex::Map))
    {
        /// All types are Map
        if (type_indexes.size() == 1)
        {
            DataTypes key_types;
            DataTypes value_types;
            key_types.reserve(types.size());
            value_types.reserve(types.size());
            for (const auto & type : types)
            {
                const DataTypeMap * type_map = typeid_cast<const DataTypeMap *>(type.get());
                key_types.emplace_back(type_map->getKeyType());
                value_types.emplace_back(type_map->getValueType());
            }

            transformTypesRecursively(key_types, transform_simple_types, transform_complex_types, format_settings);
            transformTypesRecursively(value_types, transform_simple_types, transform_complex_types, format_settings);

            for (size_t i = 0; i != types.size(); ++i)
                types[i] = std::make_shared<DataTypeMap>(key_types[i], value_types[i]);
        }

        if (transform_complex_types)
            transform_complex_types(types, type_indexes);

        return;
    }

    transform_simple_types(types, type_indexes);
}

namespace
{

/// Applies `callback` to the simple types nested in `type` and rebuilds the enclosing containers
/// around any type the callback replaced. Returns nullptr when nothing was replaced, so an untouched
/// subtree keeps its original type object together with its customizations (a custom name such as
/// `Point`, a custom serialization).
DataTypePtr replaceNestedSimpleTypes(const DataTypePtr & type, const std::function<void(DataTypePtr &)> & callback)
{
    DataTypePtr replacement;

    if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        if (auto new_nested = replaceNestedSimpleTypes(type_nullable->getNestedType(), callback))
            replacement = std::make_shared<DataTypeNullable>(new_nested);
    }
    else if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        if (auto new_nested = replaceNestedSimpleTypes(type_array->getNestedType(), callback))
        {
            replacement = std::make_shared<DataTypeArray>(new_nested);

            /// `Nested` semantics - `isNested`, subcolumn resolution, the printed name - key off the
            /// custom name, so it must survive the rebuild.
            if (isNested(type))
            {
                if (const auto * new_tuple = typeid_cast<const DataTypeTuple *>(new_nested.get()))
                    replacement->setCustomization(std::make_unique<DataTypeCustomDesc>(
                        std::make_unique<DataTypeNestedCustomName>(new_tuple->getElements(), new_tuple->getElementNames())));
            }
        }
    }
    else if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes new_elements = type_tuple->getElements();
        bool any_replaced = false;
        for (auto & element : new_elements)
        {
            if (auto new_element = replaceNestedSimpleTypes(element, callback))
            {
                element = new_element;
                any_replaced = true;
            }
        }

        if (any_replaced)
        {
            if (type_tuple->hasExplicitNames())
                replacement = std::make_shared<DataTypeTuple>(new_elements, type_tuple->getElementNames());
            else
                replacement = std::make_shared<DataTypeTuple>(new_elements);
        }
    }
    else if (const auto * type_map = typeid_cast<const DataTypeMap *>(type.get()))
    {
        auto new_key = replaceNestedSimpleTypes(type_map->getKeyType(), callback);
        auto new_value = replaceNestedSimpleTypes(type_map->getValueType(), callback);
        if (new_key || new_value)
            replacement = std::make_shared<DataTypeMap>(
                new_key ? new_key : type_map->getKeyType(), new_value ? new_value : type_map->getValueType());
    }
    else if (const auto * type_variant = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        DataTypes new_variants = type_variant->getVariants();
        bool any_replaced = false;
        for (auto & variant : new_variants)
        {
            if (auto new_variant = replaceNestedSimpleTypes(variant, callback))
            {
                variant = new_variant;
                any_replaced = true;
            }
        }

        if (any_replaced)
        {
            /// A replacement changes a name (e.g. the alternative gains a version prefix), so two
            /// alternatives that differed only in that name can collapse into one. Keeping the type
            /// unreplaced instead would announce the wrong nested types (e.g. an aggregate function
            /// state version other than the one the payload is written with), so this must be an
            /// error, not a silent no-op.
            std::unordered_set<String> names;
            for (const auto & variant : new_variants)
                if (!names.insert(variant->getName()).second)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot rewrite the nested types of {}: two of its alternatives would become the same type {}",
                        type->getName(), variant->getName());

            /// The default DataTypeVariant constructor re-sorts the alternatives by name, which after a
            /// rename would silently permute the discriminators of an existing column. Keep the original
            /// order.
            replacement = std::make_shared<DataTypeVariant>(new_variants, DataTypeVariant::FixedDiscriminatorOrder{});
        }
    }
    else
    {
        DataTypePtr maybe_replaced = type;
        callback(maybe_replaced);
        if (maybe_replaced != type)
            replacement = maybe_replaced;
    }

    if (!replacement)
        return nullptr;

    /// `SimpleAggregateFunction` keeps a separate copy of its argument types in its custom name.
    /// Rebuild that copy for every storage wrapper, not only Array: otherwise a replacement below
    /// Tuple or Map would discard the custom name or announce an aggregate-state version different
    /// from the payload written for an older peer.
    if (const auto * simple = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName()))
    {
        DataTypes new_argument_types = simple->getArgumentsDataTypes();
        for (auto & argument_type : new_argument_types)
            if (auto new_argument_type = replaceNestedSimpleTypes(argument_type, callback))
                argument_type = new_argument_type;

        replacement->setCustomization(std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomSimpleAggregateFunction>(
                simple->getFunction(), new_argument_types, simple->getParameters())));
    }

    /// A custom name is part of the observable type, so a wrapper whose custom name cannot be
    /// faithfully rebuilt (the callback is responsible for the custom names of the types it replaces)
    /// is kept as it was rather than replaced with a type that lies about its name.
    if (type->hasCustomName() && !replacement->hasCustomName())
        return nullptr;

    return replacement;
}

}

void callOnNestedSimpleTypes(DataTypePtr & type, std::function<void(DataTypePtr &)> callback)
{
    if (auto replaced = replaceNestedSimpleTypes(type, callback))
        type = replaced;
}

namespace
{

bool hasEqualsAmbiguousAlternatives(const DataTypes & alternatives)
{
    for (size_t i = 0; i + 1 < alternatives.size(); ++i)
        for (size_t j = i + 1; j < alternatives.size(); ++j)
            if (alternatives[i]->equals(*alternatives[j]))
                return true;
    return false;
}

DataTypePtr replaceNestedTypesInPairImpl(
    const DataTypePtr & left, const DataTypePtr & right, const PairedLeafCallback & on_leaf, bool skip_custom_name)
{
    const WhichDataType left_which(left);
    const WhichDataType right_which(right);

    /// Neither wrapper hides a leaf, so a pair differing only by one of them still has to be walked.
    if (right_which.isLowCardinality() && !left_which.isLowCardinality())
        return replaceNestedTypesInPairImpl(left, removeLowCardinality(right), on_leaf, skip_custom_name);
    if (right_which.isNullable() && !left_which.isNullable())
        return replaceNestedTypesInPairImpl(left, removeNullable(right), on_leaf, skip_custom_name);

    const bool descendable = left_which.isNullable() || left_which.isLowCardinality() || left_which.isArray()
        || left_which.isMap() || left_which.isTuple() || left_which.isVariant();

    if (!descendable)
        return on_leaf(left, right);

    /// Rebuilding below cannot keep the name, so hand back `right` whole, and only once the children clear:
    /// restoring a customization over children that did not is how a relabel changes what a level means.
    if (!skip_custom_name && (left->hasCustomName() || right->hasCustomName()) && left->getName() != right->getName())
    {
        if (!left->equals(*right))
            return nullptr;
        return replaceNestedTypesInPairImpl(left, right, on_leaf, /*skip_custom_name=*/ true) ? right : nullptr;
    }

    if (left_which.isNullable())
    {
        const DataTypePtr left_nested = removeNullable(left);
        auto nested = replaceNestedTypesInPairImpl(left_nested, removeNullable(right), on_leaf, false);
        if (!nested)
            return nullptr;
        return nested.get() == left_nested.get() ? left : makeNullable(nested);
    }

    if (left_which.isLowCardinality())
    {
        const DataTypePtr left_nested = removeLowCardinality(left);
        auto nested = replaceNestedTypesInPairImpl(left_nested, removeLowCardinality(right), on_leaf, false);
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
        auto nested = replaceNestedTypesInPairImpl(left_nested, right_array->getNestedType(), on_leaf, false);
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
        auto key = replaceNestedTypesInPairImpl(left_map.getKeyType(), right_map->getKeyType(), on_leaf, false);
        auto value = replaceNestedTypesInPairImpl(left_map.getValueType(), right_map->getValueType(), on_leaf, false);
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
        DataTypes elements;
        elements.reserve(left_tuple.getElements().size());
        bool moved = false;
        for (size_t i = 0; i < left_tuple.getElements().size(); ++i)
        {
            auto nested = replaceNestedTypesInPairImpl(left_tuple.getElements()[i], right_tuple->getElements()[i], on_leaf, false);
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
        auto nested = replaceNestedTypesInPairImpl(left_variant.getVariants()[i], right_variant->getVariants()[i], on_leaf, false);
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

DataTypePtr replaceNestedTypesInPair(const DataTypePtr & left, const DataTypePtr & right, const PairedLeafCallback & on_leaf)
{
    return replaceNestedTypesInPairImpl(left, right, on_leaf, /*skip_custom_name=*/ false);
}

}
