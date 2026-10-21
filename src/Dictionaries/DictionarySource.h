#pragma once

#include <memory>
#include <Columns/IColumn_fwd.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Dictionaries/IDictionary.h>
#include <boost/noncopyable.hpp>


namespace DB
{

class DictionarySource;

class DictionarySourceCoordinator final : public std::enable_shared_from_this<DictionarySourceCoordinator>
                                        , private boost::noncopyable
{
public:
    using ReadColumnsFunc = std::function<Columns (const Strings &, const DataTypes &, const Columns &, const DataTypes &, const Columns &)>;

    Pipe read(size_t num_streams);

    explicit DictionarySourceCoordinator(
        std::shared_ptr<const IDictionary> dictionary_,
        const Names & column_names,
        ColumnsWithTypeAndName && key_columns_with_type_,
        size_t max_block_size_)
        : dictionary(std::move(dictionary_))
        , key_columns_with_type(std::move(key_columns_with_type_))
        , max_block_size(max_block_size_)
        , read_columns_func([this](
            const Strings & attribute_names,
            const DataTypes & result_types,
            const Columns & key_columns,
            const DataTypes & key_types,
            const Columns & default_values_columns)
        {
            return dictionary->getColumns(attribute_names, result_types, key_columns, key_types, default_values_columns);
        })
    {
        initialize(column_names);
    }

    explicit DictionarySourceCoordinator(
        std::shared_ptr<const IDictionary> dictionary_,
        const Names & column_names,
        ColumnsWithTypeAndName && key_columns_with_type_,
        ColumnsWithTypeAndName && data_columns_with_type_,
        size_t max_block_size_)
        : dictionary(std::move(dictionary_))
        , key_columns_with_type(std::move(key_columns_with_type_))
        , data_columns_with_type(std::move(data_columns_with_type_))
        , max_block_size(max_block_size_)
        , read_columns_func([this](
            const Strings & attribute_names,
            const DataTypes & result_types,
            const Columns & key_columns,
            const DataTypes & key_types,
            const Columns & default_values_columns)
        {
            return dictionary->getColumns(attribute_names, result_types, key_columns, key_types, default_values_columns);
        })
    {
        initialize(column_names);
    }

    explicit DictionarySourceCoordinator(
        std::shared_ptr<const IDictionary> dictionary_,
        const Names & column_names,
        ColumnsWithTypeAndName && key_columns_with_type_,
        ColumnsWithTypeAndName && data_columns_with_type_,
        size_t max_block_size_,
        ReadColumnsFunc read_columns_func_)
        : dictionary(std::move(dictionary_))
        , key_columns_with_type(std::move(key_columns_with_type_))
        , data_columns_with_type(std::move(data_columns_with_type_))
        , max_block_size(max_block_size_)
        , read_columns_func(std::move(read_columns_func_))
    {
        initialize(column_names);
    }

private:

    friend class DictionarySource;

    bool getKeyColumnsNextRangeToRead(ColumnsWithTypeAndName & key_columns, ColumnsWithTypeAndName & data_columns);

    const SharedHeader & getHeader() const { return header; }

    const Strings & getAttributesNamesToRead() const { return attributes_names_to_read; }

    const DataTypes & getAttributesTypesToRead() const { return attributes_types_to_read; }

    const Columns & getAttributesDefaultValuesColumns() const { return attributes_default_values_columns; }

    const ReadColumnsFunc & getReadColumnsFunc() const { return read_columns_func; }

    const std::shared_ptr<const IDictionary> & getDictionary() const { return dictionary; }

    void initialize(const Names & column_names);

    static ColumnsWithTypeAndName cutColumns(const ColumnsWithTypeAndName & columns_with_type, size_t start, size_t length);

    std::shared_ptr<const IDictionary> dictionary;

    ColumnsWithTypeAndName key_columns_with_type;
    ColumnsWithTypeAndName data_columns_with_type;

    SharedHeader header;

    Strings attributes_names_to_read;
    DataTypes attributes_types_to_read;
    Columns attributes_default_values_columns;

    const size_t max_block_size;
    ReadColumnsFunc read_columns_func;

    std::atomic<size_t> parallel_read_block_index = 0;
};

}
