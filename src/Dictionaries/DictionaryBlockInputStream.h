#pragma once

#include <memory>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/Names.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <common/logger_useful.h>
#include "DictionaryBlockInputStreamBase.h"
#include "DictionaryStructure.h"
#include "IDictionary.h"

namespace DB
{

/// TODO: Remove this class
/* BlockInputStream implementation for external dictionaries
 * read() returns blocks consisting of the in-memory contents of the dictionaries
 */
class DictionaryBlockInputStream : public DictionaryBlockInputStreamBase
{
public:
    DictionaryBlockInputStream(
        std::shared_ptr<const IDictionary> dictionary,
        UInt64 max_block_size,
        PaddedPODArray<UInt64> && ids,
        const Names & column_names);

    DictionaryBlockInputStream(
        std::shared_ptr<const IDictionary> dictionary,
        UInt64 max_block_size,
        const PaddedPODArray<StringRef> & keys,
        const Names & column_names);

    using GetColumnsFunction = std::function<ColumnsWithTypeAndName(const Columns &, const std::vector<DictionaryAttribute> & attributes)>;

    // Used to separate key columns format for storage and view.
    // Calls get_key_columns_function to get key column for dictionary get function call
    // and get_view_columns_function to get key representation.
    // Now used in trie dictionary, where columns are stored as ip and mask, and are showed as string
    DictionaryBlockInputStream(
        std::shared_ptr<const IDictionary> dictionary,
        UInt64 max_block_size,
        const Columns & data_columns,
        const Names & column_names,
        GetColumnsFunction && get_key_columns_function,
        GetColumnsFunction && get_view_columns_function);

    String getName() const override { return "Dictionary"; }

protected:
    Block getBlock(size_t start, size_t length) const override;

private:
    Block fillBlock(
        const PaddedPODArray<UInt64> & ids_to_fill,
        const Columns & keys,
        const DataTypes & types,
        ColumnsWithTypeAndName && view) const;

    static ColumnPtr getColumnFromIds(const PaddedPODArray<UInt64> & ids_to_fill);

    static void fillKeyColumns(
        const PaddedPODArray<StringRef> & keys,
        size_t start,
        size_t size,
        const DictionaryStructure & dictionary_structure,
        ColumnsWithTypeAndName & result);

    std::shared_ptr<const IDictionary> dictionary;
    Names column_names;
    PaddedPODArray<UInt64> ids;
    ColumnsWithTypeAndName key_columns;

    Columns data_columns;
    GetColumnsFunction get_key_columns_function;
    GetColumnsFunction get_view_columns_function;

    enum class DictionaryInputStreamKeyType
    {
        Id,
        ComplexKey,
        Callback
    };

    DictionaryInputStreamKeyType key_type;
};

}
