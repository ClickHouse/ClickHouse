#include <DataTypes/transformTypesRecursively.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

TypeIndexesSet getTypesIndexes(const DataTypes & types)
{
    TypeIndexesSet type_indexes;
    for (const auto & type : types)
        type_indexes.insert(type->getTypeId());
    return type_indexes;
}

void transformTypesRecursively(DataTypes & types, std::function<void(DataTypes &, TypeIndexesSet &)> transform_simple_types, std::function<void(DataTypes &, TypeIndexesSet &)> transform_complex_types)
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

        transformTypesRecursively(nested_types, transform_simple_types, transform_complex_types);
        for (size_t i = 0; i != types.size(); ++i)
        {
            /// Type could be changed so it cannot be inside Nullable anymore.
            if (is_nullable[i] && nested_types[i]->canBeInsideNullable())
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

            transformTypesRecursively(nested_types, transform_simple_types, transform_complex_types);
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
            bool have_explicit_names = type_tuple->haveExplicitNames();
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
                    transformTypesRecursively(nested_types[elem_idx], transform_simple_types, transform_complex_types);
                    for (size_t i = 0; i != types.size(); ++i)
                        transposed_nested_types[i].push_back(nested_types[elem_idx][i]);
                }

                for (size_t i = 0; i != types.size(); ++i)
                {
                    if (have_explicit_names)
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

            transformTypesRecursively(key_types, transform_simple_types, transform_complex_types);
            transformTypesRecursively(value_types, transform_simple_types, transform_complex_types);

            for (size_t i = 0; i != types.size(); ++i)
                types[i] = std::make_shared<DataTypeMap>(key_types[i], value_types[i]);
        }

        if (transform_complex_types)
            transform_complex_types(types, type_indexes);

        return;
    }

    transform_simple_types(types, type_indexes);
}

void callOnNestedSimpleTypes(DataTypePtr & type, std::function<void(DataTypePtr &)> callback)
{
    DataTypes types = {type};
    transformTypesRecursively(types, [callback](auto & data_types, TypeIndexesSet &){ callback(data_types[0]); }, {});
}

}
