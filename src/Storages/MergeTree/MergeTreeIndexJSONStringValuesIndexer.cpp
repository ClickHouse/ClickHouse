#include <Storages/MergeTree/MergeTreeIndexJSONStringValuesIndexer.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnVariant.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesCache.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

JSONStringValuesIndexer::JSONStringValuesIndexer(MergeTreeIndexTextGranuleBuilder & granule_builder_)
    : granule_builder(granule_builder_)
{
}

void JSONStringValuesIndexer::addRow(const ColumnObject & column_object, const DataTypeObject & type_object, size_t row)
{
    token_position = 0;
    processObject({}, column_object, type_object, row);
    granule_builder.incrementCurrentRow();
}

void JSONStringValuesIndexer::emitString(std::string_view path, std::string_view value)
{
    if (value.empty())
        return;

    if (path.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot build jsonStringValues text index for an empty JSON path");

    forEachToken(
        split,
        value.data(),
        value.size(),
        [&](const char * token_data, size_t token_size)
        {
            if (token_size == 0)
                return false;

            KeyValuePairsTokenizer::encodeToken(path, std::string_view(token_data, token_size), /*is_rest=*/ false, token);
            granule_builder.addToken({reinterpret_cast<const char *>(token.data()), token.size()}, token_position++);
            return false;
        });
}

void JSONStringValuesIndexer::processValue(std::string_view path, const IColumn & column, const DataTypePtr & type, size_t row)
{
    if (column.isNullAt(row))
        return;

    const DataTypePtr unwrapped = removeNullable(removeLowCardinality(type));
    const WhichDataType which(unwrapped);

    if (which.isString())
    {
        emitString(path, column.getDataAt(row));
        return;
    }

    if (which.isObject())
    {
        const IColumn * object_column = &column;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
            object_column = &nullable->getNestedColumn();

        processObject(
            path,
            assert_cast<const ColumnObject &>(*object_column),
            assert_cast<const DataTypeObject &>(*unwrapped),
            row);
        return;
    }

    if (which.isDynamic())
    {
        const IColumn * dynamic_column = &column;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
            dynamic_column = &nullable->getNestedColumn();

        processDynamic(path, assert_cast<const ColumnDynamic &>(*dynamic_column), row);
    }
}

void JSONStringValuesIndexer::processObject(
    std::string_view prefix, const ColumnObject & column_object, const DataTypeObject & type_object, size_t row)
{
    /// Child ColumnObject paths are relative to this object. Prefix with the already-normalized
    /// parent path bytes; do not assume the child stored a rooted path.
    auto visit = [&](std::string_view child_path, const auto & fn)
    {
        if (prefix.empty())
        {
            fn(child_path);
            return;
        }

        String full_path;
        full_path.reserve(prefix.size() + 1 + child_path.size());
        full_path.append(prefix);
        full_path.push_back('.');
        full_path.append(child_path);
        fn(full_path);
    };

    const auto & typed_path_types = type_object.getTypedPaths();
    const auto & typed_path_columns = column_object.getTypedPaths();
    for (const auto & [path, type] : typed_path_types)
        visit(path, [&](std::string_view full_path) { processValue(full_path, *typed_path_columns.at(path), type, row); });

    const auto dynamic_type = column_object.getDynamicType();
    for (const auto & [path, column] : column_object.getDynamicPaths())
        visit(path, [&](std::string_view full_path) { processValue(full_path, *column, dynamic_type, row); });

    const auto & shared_data_offsets = column_object.getSharedDataOffsets();
    const auto [shared_data_paths, shared_data_values] = column_object.getSharedDataPathsAndValues();
    const size_t start = shared_data_offsets[static_cast<ssize_t>(row) - 1];
    const size_t end = shared_data_offsets[static_cast<ssize_t>(row)];
    for (size_t j = start; j != end; ++j)
        visit(shared_data_paths->getDataAt(j), [&](std::string_view full_path)
        {
            processSharedDataValue(full_path, shared_data_values->getDataAt(j));
        });
}

void JSONStringValuesIndexer::processDynamic(std::string_view path, const ColumnDynamic & column_dynamic, size_t row)
{
    const auto & variant = column_dynamic.getVariantColumn();
    const auto global_disc = variant.globalDiscriminatorAt(row);
    if (global_disc == ColumnVariant::NULL_DISCRIMINATOR)
        return;

    if (global_disc == column_dynamic.getSharedVariantDiscriminator())
    {
        processSharedDataValue(path, column_dynamic.getSharedVariant().getDataAt(variant.offsetAt(row)));
        return;
    }

    const auto & variant_type = assert_cast<const DataTypeVariant &>(*column_dynamic.getVariantInfo().variant_type);
    const auto & nested_type = variant_type.getVariants().at(global_disc);
    const auto & nested_column = variant.getVariantByGlobalDiscriminator(global_disc);
    processValue(path, nested_column, nested_type, variant.offsetAt(row));
}

void JSONStringValuesIndexer::processSharedDataValue(std::string_view path, std::string_view value_data)
{
    ReadBufferFromMemory buf(value_data);
    FormatSettings format_settings;

    auto get_serialization_from_cache = [&](const String & type_name, const IDataType & type) -> const SerializationPtr &
    {
        auto [it, inserted] = shared_serializations_cache.try_emplace(type_name);
        if (inserted)
            it->second = type.getDefaultSerialization();
        return it->second;
    };

    auto get_column_from_cache = [&](const String & type_name, const IDataType & type) -> const MutableColumnPtr &
    {
        auto [it, inserted] = shared_columns_cache.try_emplace(type_name);
        if (inserted)
            it->second = type.createColumn();
        return it->second;
    };

    auto process_decoded = [&](const DataTypePtr & type, const ISerialization & serialization, IColumn & temp_column)
    {
        if (isNothing(type))
            return;

        serialization.deserializeBinary(temp_column, buf, format_settings);
        processValue(path, temp_column, type, 0);
        temp_column.popBack(1);
    };

    char type_index = 0;
    if (!buf.peek(type_index))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse shared data value of JSON: no type index found");

    const auto & cache = getSimpleDataTypesCache();
    auto binary_type_index = static_cast<BinaryTypeIndex>(type_index);

    if (cache.hasElement(binary_type_index))
    {
        ++buf.position();
        const auto & element = cache.getElement(binary_type_index);
        const auto & temp_column = get_column_from_cache(element.name, *element.type);
        process_decoded(element.type, *element.serialization, *temp_column);
    }
    else
    {
        auto type = decodeDataType(buf);
        auto type_name = type->getName();
        const auto & serialization = get_serialization_from_cache(type_name, *type);
        const auto & temp_column = get_column_from_cache(type_name, *type);
        process_decoded(type, *serialization, *temp_column);
    }
}

}
