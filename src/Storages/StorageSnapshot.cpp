#include <Storages/StorageSnapshot.h>
#include <Storages/IStorage.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/NestedUtils.h>
#include <Storages/StorageView.h>
#include <sparsehash/dense_hash_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
}

StorageSnapshot::StorageSnapshot(
    const IStorage & storage_,
    StorageMetadataPtr metadata_)
    : storage(storage_)
    , metadata(std::move(metadata_))
    , virtual_columns(storage_.getVirtualsPtr())
{
}

StorageSnapshot::StorageSnapshot(
    const IStorage & storage_,
    StorageMetadataPtr metadata_,
    VirtualsDescriptionPtr virtual_columns_)
    : storage(storage_)
    , metadata(std::move(metadata_))
    , virtual_columns(std::move(virtual_columns_))
{
}

StorageSnapshot::StorageSnapshot(
    const IStorage & storage_,
    StorageMetadataPtr metadata_,
    ColumnsDescription object_columns_)
    : storage(storage_)
    , metadata(std::move(metadata_))
    , virtual_columns(storage_.getVirtualsPtr())
    , object_columns(std::move(object_columns_))
{
}

StorageSnapshot::StorageSnapshot(
    const IStorage & storage_,
    StorageMetadataPtr metadata_,
    ColumnsDescription object_columns_,
    DataPtr data_)
    : storage(storage_)
    , metadata(std::move(metadata_))
    , virtual_columns(storage_.getVirtualsPtr())
    , object_columns(std::move(object_columns_))
    , data(std::move(data_))
{
}

std::shared_ptr<StorageSnapshot> StorageSnapshot::clone(DataPtr data_) const
{
    auto res = std::make_shared<StorageSnapshot>(storage, metadata, object_columns);

    res->data = std::move(data_);

    return res;
}

ColumnsDescription StorageSnapshot::getAllColumnsDescription() const
{
    auto get_column_options = GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects().withVirtuals();
    auto column_names_and_types = getColumns(get_column_options);

    return ColumnsDescription{column_names_and_types};
}

NamesAndTypesList StorageSnapshot::getColumns(const GetColumnsOptions & options) const
{
    auto all_columns = metadata->getColumns().get(options);

    if (options.with_extended_objects)
        extendObjectColumns(all_columns, object_columns, options.with_subcolumns);

    if (options.virtuals_kind != VirtualsKind::None && !virtual_columns->empty())
    {
        NameSet column_names;
        for (const auto & column : all_columns)
            column_names.insert(column.name);

        auto virtuals_list = virtual_columns->getNamesAndTypesList(options.virtuals_kind);
        for (const auto & column : virtuals_list)
        {
            if (column_names.contains(column.name))
                continue;

            all_columns.emplace_back(column.name, column.type);
        }
    }

    return all_columns;
}

NamesAndTypesList StorageSnapshot::getColumnsByNames(const GetColumnsOptions & options, const Names & names) const
{
    NamesAndTypesList res;
    for (const auto & name : names)
        res.push_back(getColumn(options, name));
    return res;
}

std::optional<NameAndTypePair> StorageSnapshot::tryGetColumn(const GetColumnsOptions & options, const String & column_name) const
{
    const auto & columns = metadata->getColumns();
    auto column = columns.tryGetColumn(options, column_name);
    if (column && (!column->type->hasDynamicSubcolumnsDeprecated() || !options.with_extended_objects))
        return column;

    if (options.with_extended_objects)
    {
        auto object_column = object_columns.tryGetColumn(options, column_name);
        if (object_column)
            return object_column;
    }

    if (options.virtuals_kind != VirtualsKind::None)
    {
        auto virtual_column = virtual_columns->tryGet(column_name, options.virtuals_kind);
        if (virtual_column)
            return NameAndTypePair{virtual_column->name, virtual_column->type};
    }

    return {};
}

NameAndTypePair StorageSnapshot::getColumn(const GetColumnsOptions & options, const String & column_name) const
{
    auto column = tryGetColumn(options, column_name);
    if (!column)
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no column {} in table", column_name);

    return *column;
}

CompressionCodecPtr StorageSnapshot::getCodecOrDefault(const String & column_name, CompressionCodecPtr default_codec) const
{
    auto get_codec_or_default = [&](const auto & column_desc)
    {
        return column_desc.codec
            ? CompressionCodecFactory::instance().get(column_desc.codec, column_desc.type, default_codec)
            : default_codec;
    };

    const auto & columns = metadata->getColumns();
    if (const auto * column_desc = columns.tryGet(column_name))
        return get_codec_or_default(*column_desc);

    if (const auto * virtual_desc = virtual_columns->tryGetDescription(column_name))
        return get_codec_or_default(*virtual_desc);

    return default_codec;
}

CompressionCodecPtr StorageSnapshot::getCodecOrDefault(const String & column_name) const
{
    return getCodecOrDefault(column_name, CompressionCodecFactory::instance().getDefaultCodec());
}

ASTPtr StorageSnapshot::getCodecDescOrDefault(const String & column_name, CompressionCodecPtr default_codec) const
{
    auto get_codec_or_default = [&](const auto & column_desc)
    {
        return column_desc.codec ? column_desc.codec : default_codec->getFullCodecDesc();
    };

    const auto & columns = metadata->getColumns();
    if (const auto * column_desc = columns.tryGet(column_name))
        return get_codec_or_default(*column_desc);

    if (const auto * virtual_desc = virtual_columns->tryGetDescription(column_name))
        return get_codec_or_default(*virtual_desc);

    return default_codec->getFullCodecDesc();
}

Block StorageSnapshot::getSampleBlockForColumns(const Names & column_names) const
{
    Block res;

    const auto & columns = metadata->getColumns();
    for (const auto & column_name : column_names)
    {
        auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, column_name);
        auto object_column = object_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, column_name);
        if (column && !object_column)
        {
            res.insert({column->type->createColumn(), column->type, column_name});
        }
        else if (object_column)
        {
            res.insert({object_column->type->createColumn(), object_column->type, column_name});
        }
        else if (auto virtual_column = virtual_columns->tryGet(column_name))
        {
            /// Virtual columns must be appended after ordinary, because user can
            /// override them.
            const auto & type = virtual_column->type;
            res.insert({type->createColumn(), type, column_name});
        }
        else
        {
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                "Column {} not found in table {}", backQuote(column_name), storage.getStorageID().getNameForLogs());
        }
    }
    return res;
}

ColumnsDescription StorageSnapshot::getDescriptionForColumns(const Names & column_names) const
{
    ColumnsDescription res;
    const auto & columns = metadata->getColumns();
    for (const auto & name : column_names)
    {
        auto column = columns.tryGetColumnOrSubcolumnDescription(GetColumnsOptions::All, name);
        auto object_column = object_columns.tryGetColumnOrSubcolumnDescription(GetColumnsOptions::All, name);
        if (column && !object_column)
        {
            res.add(*column, "", false, false);
        }
        else if (object_column)
        {
            res.add(*object_column, "", false, false);
        }
        else if (auto virtual_column = virtual_columns->tryGet(name))
        {
            /// Virtual columns must be appended after ordinary, because user can
            /// override them.
            res.add({name, virtual_column->type});
        }
        else
        {
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                            "Column {} not found in table {}", backQuote(name), storage.getStorageID().getNameForLogs());
        }
    }

    return res;
}

namespace
{
    using DenseHashSet = google::dense_hash_set<StringRef, StringRefHash>;
}

void StorageSnapshot::check(const Names & column_names) const
{
    const auto & columns = metadata->getColumns();
    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns();

    if (column_names.empty())
    {
        auto list_of_columns = listOfColumns(columns.get(options));
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
            "Empty list of columns queried. There are columns: {}", list_of_columns);
    }

    DenseHashSet unique_names;
    unique_names.set_empty_key(StringRef());

    for (const auto & name : column_names)
    {
        bool has_column = columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name)
            || object_columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name)
            || virtual_columns->has(name);

        if (!has_column)
        {
            auto list_of_columns = listOfColumns(columns.get(options));
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                "There is no column with name {} in table {}. There are columns: {}",
                backQuote(name), storage.getStorageID().getNameForLogs(), list_of_columns);
        }

        if (unique_names.count(name))
            throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Column {} queried more than once", name);

        unique_names.insert(name);
    }
}

DataTypePtr StorageSnapshot::getConcreteType(const String & column_name) const
{
    auto object_column = object_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, column_name);
    if (object_column)
        return object_column->type;

    return metadata->getColumns().get(column_name).type;
}

}
