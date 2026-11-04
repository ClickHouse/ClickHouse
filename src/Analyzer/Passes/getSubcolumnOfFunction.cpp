#include <Analyzer/Passes/getSubcolumnOfFunction.h>

#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

/// Two substreams are the same step only if they are the same kind AND name the same element: `type`
/// alone cannot tell ``TupleElement(j)`` from ``TupleElement(`j.m`)``, nor one JSON path from another.
bool isSameSubstream(const ISerialization::Substream & lhs, const ISerialization::Substream & rhs)
{
    return lhs.type == rhs.type && lhs.name_of_substream == rhs.name_of_substream
        && lhs.variant_element_name == rhs.variant_element_name
        && lhs.object_path_name == rhs.object_path_name && lhs.bucket == rhs.bucket;
}

/// The type of the `size` subcolumn of a String or Nullable(String) column.
/// For Nullable(String) it is Nullable(UInt64) with NULL in the rows where the column is NULL,
/// which is the same as the result of `length`, `empty` and `notEmpty` for these rows.
DataTypePtr getStringSizesSubcolumnType(const DataTypePtr & column_type)
{
    DataTypePtr type = std::make_shared<DataTypeUInt64>();
    if (column_type->isNullable())
        return makeNullable(type);
    return type;
}

std::optional<String> getElementSubcolumnName(const Field & value, const DataTypeTuple & data_type_tuple)
{
    const auto & names = data_type_tuple.getElementNames();

    if (value.getType() == Field::Types::String)
    {
        const auto & name = value.safeGet<String>();
        auto pos = data_type_tuple.tryGetPositionByName(name);

        if (!pos)
            return {};

        return name;
    }

    if (value.getType() == Field::Types::UInt64)
    {
        size_t index = value.safeGet<UInt64>();

        if (index == 0 || index > names.size())
            return {};

        return names[index - 1];
    }

    /// Signed indices can address elements from the end of the tuple.
    if (value.getType() == Field::Types::Int64)
    {
        ssize_t index = value.safeGet<Int64>();
        ssize_t size = names.size();

        if (index == 0 || index < -size || index > size)
            return {};

        if (index > 0)
            return names[index - 1];
        else
            return names[size + index];
    }

    return {};
}

std::optional<String> getElementSubcolumnName(const Field & value, const DataTypeVariant & data_type_variant)
{
    if (value.getType() != Field::Types::String)
        return {};

    const auto & name = value.safeGet<String>();
    auto discr = data_type_variant.tryGetVariantDiscriminator(name);

    if (!discr)
        return {};

    return name;
}

std::optional<String> getElementSubcolumnName(const Field & value, const DataTypeQBit & data_type_qbit)
{
    size_t index = 0;

    if (value.getType() == Field::Types::UInt64)
        index = value.safeGet<UInt64>();
    else
        return {};

    if (index == 0 || index > data_type_qbit.getElementSize() * data_type_qbit.getNumStrides())
        return {};

    return toString(index);
}

/// True when `element_name` and some nested path of `tuple` flatten to the same dotted name, so
/// `<column>.<element_name>` no longer identifies one element: an exact element-name lookup binds
/// the dotted element while a prefix walk over a file schema binds the nested one.
bool tupleElementNameIsAmbiguousWhenFlattened(const DataTypeTuple & tuple, const String & element_name)
{
    std::string_view name = element_name;
    for (size_t dot = name.find('.'); dot != std::string_view::npos; dot = name.find('.', dot + 1))
    {
        auto head = name.substr(0, dot);
        auto tail = name.substr(dot + 1);
        /// Case-insensitively, because a reader may match field names that way.
        if (!head.empty() && !tail.empty()
            && (tuple.tryGetPositionByName(head) || tuple.tryGetPositionByName(head, /*case_insensitive=*/true)))
            return true;
    }
    return false;
}

std::optional<String> tryGetMapKeySubcolumnName(const DataTypeMap & data_type_map, const Field & key)
{
    const auto & key_type = data_type_map.getKeyType();

    Field key_value = key;
    if (isEnum(key_type))
    {
        /// A map with Enum keys can be indexed either by the name of the enum value or by its numeric value.
        /// Convert both forms to the numeric value of the enum. This also rejects a value that is not in
        /// the Enum at all, which has no name and hence no key subcolumn, while `arrayElement` returns
        /// the default value for it.
        key_value = tryConvertFieldToType(key_value, *key_type);
        if (key_value.isNull())
            return {};
    }

    auto tmp_key_column = key_type->createColumn();
    /// Verify that the constant value is compatible with the map's key type.
    if (!tmp_key_column->tryInsert(key_value))
        return {};

    /// Serialize the key to its text representation to construct the subcolumn name,
    /// e.g. the string key "foo" becomes the subcolumn suffix "key_foo".
    WriteBufferFromOwnString buf;
    key_type->getDefaultSerialization()->serializeText(*tmp_key_column, 0, buf, FormatSettings());
    return String(DataTypeMap::KEY_SUBCOLUMN_PREFIX) + buf.str();
}

}
