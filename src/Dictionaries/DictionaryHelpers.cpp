#include <Dictionaries/DictionaryHelpers.h>

namespace DB
{

MutableColumns deserializeColumnsFromKeys(
    const DictionaryStructure & dictionary_structure,
    const PaddedPODArray<StringRef> & keys,
    size_t start,
    size_t end)
{
    MutableColumns result_columns;
    result_columns.reserve(dictionary_structure.key->size());

    for (const DictionaryAttribute & attribute : *dictionary_structure.key)
        result_columns.emplace_back(attribute.type->createColumn());

    for (size_t index = start; index < end; ++index)
    {
        const auto & key = keys[index];
        const auto * ptr = key.data;

        for (auto & result_column : result_columns)
            ptr = result_column->deserializeAndInsertFromArena(ptr);
    }

    return result_columns;
}

ColumnsWithTypeAndName deserializeColumnsWithTypeAndNameFromKeys(
    const DictionaryStructure & dictionary_structure,
    const PaddedPODArray<StringRef> & keys,
    size_t start,
    size_t end)
{
    ColumnsWithTypeAndName result;
    MutableColumns columns = deserializeColumnsFromKeys(dictionary_structure, keys, start, end);

    for (size_t i = 0, num_columns = columns.size(); i < num_columns; ++i)
    {
        const auto & dictionary_attribute = (*dictionary_structure.key)[i];
        result.emplace_back(ColumnWithTypeAndName{std::move(columns[i]), dictionary_attribute.type, dictionary_attribute.name});
    }

    return result;
}

}
