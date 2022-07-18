#include <DataTypes/transformTypesRecursively.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

void transformTypesRecursively(DataTypes & types, std::function<void(DataTypes &)> transform_simple_types, std::function<void(DataTypes &)> transform_complex_types)
{
    {
        /// Arrays
        bool have_array = false;
        bool all_arrays = true;
        DataTypes nested_types;
        for (const auto & type : types)
        {
            if (const DataTypeArray * type_array = typeid_cast<const DataTypeArray *>(type.get()))
            {
                have_array = true;
                nested_types.push_back(type_array->getNestedType());
            }
            else
                all_arrays = false;
        }

        if (have_array)
        {
            if (all_arrays)
            {
                transformTypesRecursively(nested_types, transform_simple_types, transform_complex_types);
                for (size_t i = 0; i != types.size(); ++i)
                    types[i] = std::make_shared<DataTypeArray>(nested_types[i]);
            }

            if (transform_complex_types)
                transform_complex_types(types);

            return;
        }
    }

    {
        /// Tuples
        bool have_tuple = false;
        bool all_tuples = true;
        size_t tuple_size = 0;

        std::vector<DataTypes> nested_types;

        for (const auto & type : types)
        {
            if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
            {
                if (!have_tuple)
                {
                    tuple_size = type_tuple->getElements().size();
                    nested_types.resize(tuple_size);
                    for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                        nested_types[elem_idx].reserve(types.size());
                }
                else if (tuple_size != type_tuple->getElements().size())
                    return;

                have_tuple = true;

                for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                    nested_types[elem_idx].emplace_back(type_tuple->getElements()[elem_idx]);
            }
            else
                all_tuples = false;
        }

        if (have_tuple)
        {
            if (all_tuples)
            {
                std::vector<DataTypes> transposed_nested_types(types.size());
                for (size_t elem_idx = 0; elem_idx < tuple_size; ++elem_idx)
                {
                    transformTypesRecursively(nested_types[elem_idx], transform_simple_types, transform_complex_types);
                    for (size_t i = 0; i != types.size(); ++i)
                        transposed_nested_types[i].push_back(nested_types[elem_idx][i]);
                }

                for (size_t i = 0; i != types.size(); ++i)
                    types[i] = std::make_shared<DataTypeTuple>(transposed_nested_types[i]);

                if (transform_complex_types)
                    transform_complex_types(types);
            }

            if (transform_complex_types)
                transform_complex_types(types);

            return;
        }
    }

    {
        /// Maps
        bool have_maps = false;
        bool all_maps = true;
        DataTypes key_types;
        DataTypes value_types;
        key_types.reserve(types.size());
        value_types.reserve(types.size());

        for (const auto & type : types)
        {
            if (const DataTypeMap * type_map = typeid_cast<const DataTypeMap *>(type.get()))
            {
                have_maps = true;
                key_types.emplace_back(type_map->getKeyType());
                value_types.emplace_back(type_map->getValueType());
            }
            else
                all_maps = false;
        }

        if (have_maps)
        {
            if (all_maps)
            {
                transformTypesRecursively(key_types, transform_simple_types, transform_complex_types);
                transformTypesRecursively(value_types, transform_simple_types, transform_complex_types);

                for (size_t i = 0; i != types.size(); ++i)
                    types[i] = std::make_shared<DataTypeMap>(key_types[i], value_types[i]);
            }

            if (transform_complex_types)
                transform_complex_types(types);

            return;
        }
    }

    {
        /// Nullable
        bool have_nullable = false;
        std::vector<UInt8> is_nullable;
        is_nullable.reserve(types.size());
        DataTypes nested_types;
        nested_types.reserve(types.size());
        for (const auto & type : types)
        {
            if (const DataTypeNullable * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
            {
                have_nullable = true;
                is_nullable.push_back(1);
                nested_types.push_back(type_nullable->getNestedType());
            }
            else
            {
                is_nullable.push_back(0);
                nested_types.push_back(type);
            }
        }

        if (have_nullable)
        {
            transformTypesRecursively(nested_types, transform_simple_types, transform_complex_types);
            for (size_t i = 0; i != types.size(); ++i)
            {
                if (is_nullable[i])
                    types[i] = makeNullable(nested_types[i]);
                else
                    types[i] = nested_types[i];
            }

            return;
        }
    }

    transform_simple_types(types);
}

}
