#include <Storages/MergeTree/MergeTreeIndexTextSetHelper.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

namespace
{

/// The aggregator tokenizes the element type of an `Array` column, so the set is compared against
/// that element type rather than the array.
DataTypePtr unwrapTextIndexType(const DataTypePtr & type)
{
    auto unwrapped = removeNullable(recursiveRemoveLowCardinality(type));
    if (const auto * array = typeid_cast<const DataTypeArray *>(unwrapped.get()))
        return removeNullable(recursiveRemoveLowCardinality(array->getNestedType()));
    return unwrapped;
}

bool isTextual(TypeIndex id)
{
    return id == TypeIndex::String || id == TypeIndex::FixedString;
}

/// A preprocessor over an `Array` carrier is rewritten as `arrayMap`, so it receives the element
/// type rather than the array. See `MergeTreeIndexTextPreprocessor`.
DataTypePtr preprocessorInputType(const DataTypePtr & index_type)
{
    if (const auto * array = typeid_cast<const DataTypeArray *>(index_type.get()))
        return array->getNestedType();
    return index_type;
}

}

bool textIndexSetElementIsComparable(
    const DataTypePtr & set_type,
    const DataTypePtr & index_type,
    const ITokenizer & tokenizer,
    bool has_preprocessor,
    bool preprocessor_is_case_folding)
{
    auto set_unwrapped = unwrapTextIndexType(set_type);
    auto index_unwrapped = unwrapTextIndexType(index_type);

    /// A preprocessor is applied to a set element under `String`, and to the index column under
    /// the type it receives there: the element type for an `Array` carrier, which is rewritten
    /// through `arrayMap`, and the declared type otherwise. Any other carrier lets the two
    /// applications read different input and produce different tokens, unless the preprocessor only
    /// folds case, which maps the bytes it is given without consulting their type.
    if (has_preprocessor && !preprocessor_is_case_folding
        && !preprocessorInputType(index_type)->equals(DataTypeString{}))
        return false;

    if (set_unwrapped->equals(*index_unwrapped))
        return true;

    if (!isTextual(set_unwrapped->getTypeId()) || !isTextual(index_unwrapped->getTypeId()))
        return false;

    /// The `FixedString` padding is then the only difference, and only a tokenizer that splits on
    /// non-alphanumeric bytes erases it. A preprocessor runs first and can map the padding onto
    /// ordinary token bytes, so it makes no representation interchangeable.
    return !has_preprocessor && tokenizer.getType() == ITokenizer::Type::SplitByNonAlpha;
}

}
