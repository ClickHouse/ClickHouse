#pragma once

#include <memory>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/Names.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>


namespace DB
{

class DictionarySourceCoordinator
{
public:

    explicit DictionarySourceCoordinator(
        std::shared_ptr<const IDictionary> dictionary_,
        const Names & column_names,
        ColumnsWithTypeAndName && key_columns_with_type_,
        size_t max_block_size_)
        : dictionary(std::move(dictionary_))
        , key_columns_with_type(std::move(key_columns_with_type_))
        , max_block_size(max_block_size_)
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
    {
        initialize(column_names);
    }

    bool getKeyColumnsNextRangeToRead(ColumnsWithTypeAndName & key_columns, ColumnsWithTypeAndName & data_columns);

    const Block & getHeader() const { return header; }

    const std::vector<std::string> & getAttributesNamesToRead() const { return attributes_names_to_read; }

    const std::vector<DataTypePtr> & getAttributesTypesToRead() const { return attributes_types_to_read; }

    const std::vector<ColumnPtr> & getAttributesDefaultValuesColumns() const { return attributes_default_values_columns; }

    const std::shared_ptr<const IDictionary> & getDictionary() const { return dictionary; }

private:
    void initialize(const Names & column_names);

    static ColumnsWithTypeAndName cutColumns(const ColumnsWithTypeAndName & columns_with_type, size_t start, size_t length);

    std::shared_ptr<const IDictionary> dictionary;

    ColumnsWithTypeAndName key_columns_with_type;
    ColumnsWithTypeAndName data_columns_with_type;

    Block header;

    std::vector<std::string> attributes_names_to_read;
    std::vector<DataTypePtr> attributes_types_to_read;
    std::vector<ColumnPtr> attributes_default_values_columns;

    const size_t max_block_size;
    std::atomic<size_t> parallel_read_block_index = 0;
};

class DictionarySource : public SourceWithProgress
{
public:

    explicit DictionarySource(std::shared_ptr<DictionarySourceCoordinator> coordinator_)
        : SourceWithProgress(coordinator_->getHeader()), coordinator(std::move(coordinator_))
    {
    }

private:
    String getName() const override { return "DictionarySource"; }

    Chunk generate() override;

    std::shared_ptr<DictionarySourceCoordinator> coordinator;
};

}
