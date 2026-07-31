#pragma once

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>

#include <map>
#include <utility>


namespace DB
{

class ColumnsDescription;
class DataTypeTuple;

namespace Nested
{
    std::string concatenateName(const std::string & nested_table_name, const std::string & nested_field_name);

    /// Splits name of compound identifier by first/last dot (depending on 'reverse' parameter).
    /// If the name is not nested (no dot or dot at start/end),
    /// returns {name, ""}.
    std::pair<std::string, std::string> splitName(const std::string & name, bool reverse = false);
    std::pair<std::string_view, std::string_view> splitName(std::string_view name, bool reverse = false);

    /// Returns all possible pairs of column + subcolumn for specified name.
    /// For example:
    /// "a.b.c.d" -> ("a", "b.c.d"), ("a.b", "c.d"), ("a.b.c", "d")
    std::vector<std::pair<std::string_view, std::string_view>> getAllColumnAndSubcolumnPairs(std::string_view name);

    /// Given all existing columns, return specific pair of column and subcolumn from specified name.
    /// For example:
    /// Columns: "a.x", "b", "c". Name: "a.x.y.z". Result: ("a.x", "y.z").
    std::pair<std::string_view, std::string_view> getColumnAndSubcolumnPair(std::string_view name, const NameSet & storage_columns);

    /// Given all existing columns, return column name of the subcolumn with specified name.
    /// For example:
    /// Columns: "a.x", "b", "c". Name: "a.x.y.z". Result: "a.x".
    std::string_view getColumnFromSubcolumn(std::string_view name, const NameSet & storage_columns);

    /// Given all existing columns, return column name, or the name of the subcolumn with specified name in storage.
    /// Returns std::nullopt if column or subcolumn is not in the storage.
    /// For example:
    /// Columns: "a.x", "b", "c".
    /// Name: "a.x.y.z". Result: "a.x".
    /// Name: "b". Result "b";
    std::optional<String> tryGetColumnNameInStorage(const String & name, const NameSet & storage_columns);

    /// Returns the prefix of the name to the first '.'. Or the name is unchanged if there is no dot.
    std::string extractTableName(const std::string & nested_name);

    /// Flat a column of nested type into columns
    /// 1) For named tuples，t Tuple(x .., y ..., ...), replace it with t.x ..., t.y ... , ...
    /// 2) For an Nested column, a Array(Tuple(x ..., y ..., ...)), replace it with multiple Array Columns, a.x ..., a.y ..., ...
    Block flatten(const Block & block);

    /// Same as flatten but only for Nested column.
    Block flattenNested(const Block & block);

    /// Returns the Tuple type if `type` is a Tuple that recursive flattening expands into leaf
    /// columns (that is, a non-empty Tuple without a custom type name), and returns nullptr
    /// otherwise. Empty tuples (`Tuple()`) and custom-named tuples (such as `Point` or a
    /// `SimpleAggregateFunction` state) are kept as opaque leaves. This is the single predicate
    /// that defines what flattening descends into, and flatten and reconstruct must agree on it.
    const DataTypeTuple * tryGetFlattenableTuple(const DataTypePtr & type);

    /// Recursively flatten all Tuple columns in the block.
    /// For tuples with explicit names: t Tuple(x Int32, y String) -> t.x Int32, t.y String
    /// For tuples without explicit names: t Tuple(Int32, String) -> t.1 Int32, t.2 String
    /// Nested tuples are recursively flattened: t Tuple(a Int32, b Tuple(c Int64, d String))
    ///   -> t.a Int32, t.b.c Int64, t.b.d String
    /// If `flattened_ancestors` is not null, it is filled (aligned by position with the result)
    /// with each leaf's tuple ancestor paths.
    Block flattenTupleRecursive(const Block & block, std::vector<Strings> * flattened_ancestors = nullptr);

    /// All tuples are flattened recursively, regardless of whether they have explicit names.
    /// For example, [Int32, Tuple(field1 Int64, field2 String)] will be flattened to [Int32, Int64, String].
    /// Non-tuple columns are kept as-is in the result.
    Columns flattenTupleColumnsRecursive(const Block & header, const Columns & columns);

    /// Appends to `out` the leaf names that `flattenTupleRecursive` would produce for a single
    /// top-level column `(name, type)`.
    void flattenTupleLeafNames(const String & name, const DataTypePtr & type, Names & out);

    /// This is the inverse operation of flattenTupleColumnsRecursive.
    /// All tuples in the header will be reconstructed, regardless of whether they have explicit names.
    /// The header defines the expected structure (including tuple types).
    Columns reconstructTupleColumnsRecursive(const Block & header, const Columns & flattened_columns);

    /// Collect Array columns in a form of `column_name.element_name` to single Nested column.
    NamesAndTypesList collect(const NamesAndTypesList & names_and_types);

    /// Convert old-style nested (single arrays with same prefix, `n.a`, `n.b`...) to subcolumns of data type Nested.
    NamesAndTypesList convertToSubcolumns(const NamesAndTypesList & names_and_types);

    /// Unwrap Nullable(Tuple(...)) into Tuple(...) by propagating the struct-level null map
    /// to each element. Scalar elements become Nullable(T), already-Nullable elements get merged
    /// null maps, and non-nullable-compatible elements (Array, Map) get defaults at null positions.
    /// When there are no actual nulls, simply strips the Nullable wrapper.
    /// Used by format readers (Arrow, ORC) to convert Nullable struct elements for Nested flattening.
    ColumnWithTypeAndName unwrapNullableTuple(const ColumnWithTypeAndName & column);

    /// Check that sizes of arrays - elements of nested data structures - are equal.
    void validateArraySizes(const Block & block);

    /// Get all nested tables names from a block.
    std::unordered_set<String> getAllTableNames(const Block & block, bool to_lower_case = false);

    /// Extract all column names that are nested for specifying table.
    Names getAllNestedColumnsForTable(const Block & block, const std::string & table_name);

    /// Returns true if @column_name is a subcolumn (of Array type) of any Nested column in @columns.
    bool isSubcolumnOfNested(const String & column_name, const ColumnsDescription & columns);
}

/// Use this class to extract element columns from columns of nested type in a block, e.g. named Tuple.
/// It can extract a column from a multiple nested type column, e.g. named Tuple in named Tuple
/// Keeps some intermediate data to avoid rebuild them multi-times.
class NestedColumnExtractHelper
{
public:
    explicit NestedColumnExtractHelper(const Block & block_, bool case_insentive_);
    std::optional<ColumnWithTypeAndName> extractColumn(const String & column_name);
private:
    std::optional<ColumnWithTypeAndName>
    extractColumn(const String & original_column_name, const String & column_name_prefix, const String & column_name_suffix);
    const Block & block;
    bool case_insentive;
    std::map<String, BlockPtr> nested_tables;
};

/// Returns type of scalars of Array of arbitrary dimensions and takes into account Tuples of Nested.
DataTypePtr getBaseTypeOfArray(DataTypePtr type, const Names & tuple_elements);

/// Returns Array type with requested scalar type and number of dimensions.
DataTypePtr createArrayOfType(DataTypePtr type, size_t num_dimensions);

}
