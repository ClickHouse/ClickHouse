#pragma once

#include <shared_mutex>
#include <Columns/ColumnArray.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Limits.h>
#include <Interpreters/SetVariants.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/BoolMask.h>

#include <common/logger_useful.h>


namespace DB
{

struct Range;
class FieldWithInfinity;

using SetElements = std::vector<std::vector<Field>>;
using SetElementsPtr = std::unique_ptr<SetElements>;

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

/** Data structure for implementation of IN expression.
  */
class Set
{
public:
    Set(const Limits & limits) :
        log(&Logger::get("Set")),
        max_rows(limits.max_rows_in_set),
        max_bytes(limits.max_bytes_in_set),
        overflow_mode(limits.set_overflow_mode),
        set_elements(std::make_unique<SetElements>())
    {
    }

    bool empty() const { return data.empty(); }

    /** Create a Set from expression (specified literally in the query).
      * 'types' - types of what are on the left hand side of IN.
      * 'node' - list of values: 1, 2, 3 or list of tuples: (1, 2), (3, 4), (5, 6).
      * 'fill_set_elements' - if true, fill vector of elements. For primary key to work.
      */
    void createFromAST(const DataTypes & types, ASTPtr node, const Context & context, bool fill_set_elements);

    /** Returns false, if some limit was exceeded and no need to insert more data.
      */
    bool insertFromBlock(const Block & block, bool fill_set_elements);

    /** For columns of 'block', check belonging of corresponding rows to the set.
      * Return UInt8 column with the result.
      */
    ColumnPtr execute(const Block & block, bool negative) const;

    size_t getTotalRowCount() const { return data.getTotalRowCount(); }
    size_t getTotalByteCount() const { return data.getTotalByteCount(); }
    SetElements & getSetElements() { return *set_elements.get(); }

private:
    Sizes key_sizes;

    SetVariants data;

    /** How IN works with Nullable types.
      *
      * For simplicity reasons, all NULL values and any tuples with at least one NULL element are ignored in the Set.
      * And for left hand side values, that are NULLs or contain any NULLs, we return 0 (means that element is not in Set).
      *
      * If we want more standard compliant behaviour, we must return NULL
      *  if lhs is NULL and set is not empty or if lhs is not in set, but set contains at least one NULL.
      * It is more complicated with tuples.
      * For example,
      *      (1, NULL, 2) IN ((1, NULL, 3)) must return 0,
      *  but (1, NULL, 2) IN ((1, 1111, 2)) must return NULL.
      *
      * We have not implemented such sophisticated behaviour.
      */

    /** The data types from which the set was created.
      * When checking for belonging to a set, the types of columns to be checked must match with them.
      */
    DataTypes data_types;

    Logger * log;

    /// Limitations on the maximum size of the set
    size_t max_rows;
    size_t max_bytes;
    OverflowMode overflow_mode;

    /// If there is an array on the left side of IN. We check that at least one element of the array presents in the set.
    void executeArray(const ColumnArray * key_column, ColumnUInt8::Container & vec_res, bool negative) const;

    /// If in the left part columns contains the same types as the elements of the set.
    void executeOrdinary(
        const ColumnRawPtrs & key_columns,
        ColumnUInt8::Container & vec_res,
        bool negative,
        const PaddedPODArray<UInt8> * null_map) const;

    /// Check whether the permissible sizes of keys set reached
    bool checkSetSizeLimits() const;

    /// Vector of elements of `Set`.
    /// It is necessary for the index to work on the primary key in the IN statement.
    SetElementsPtr set_elements;

    /** Protects work with the set in the functions `insertFromBlock` and `execute`.
      * These functions can be called simultaneously from different threads only when using StorageSet,
      *  and StorageSet calls only these two functions.
      * Therefore, the rest of the functions for working with set are not protected.
      */
    mutable std::shared_mutex rwlock;

    template <typename Method>
    void insertFromBlockImpl(
        Method & method,
        const ColumnRawPtrs & key_columns,
        size_t rows,
        SetVariants & variants,
        ConstNullMapPtr null_map);

    template <typename Method, bool has_null_map>
    void insertFromBlockImplCase(
        Method & method,
        const ColumnRawPtrs & key_columns,
        size_t rows,
        SetVariants & variants,
        ConstNullMapPtr null_map);

    template <typename Method>
    void executeImpl(
        Method & method,
        const ColumnRawPtrs & key_columns,
        ColumnUInt8::Container & vec_res,
        bool negative,
        size_t rows,
        ConstNullMapPtr null_map) const;

    template <typename Method, bool has_null_map>
    void executeImplCase(
        Method & method,
        const ColumnRawPtrs & key_columns,
        ColumnUInt8::Container & vec_res,
        bool negative,
        size_t rows,
        ConstNullMapPtr null_map) const;

    template <typename Method>
    void executeArrayImpl(
        Method & method,
        const ColumnRawPtrs & key_columns,
        const ColumnArray::Offsets & offsets,
        ColumnUInt8::Container & vec_res,
        bool negative,
        size_t rows) const;
};

using SetPtr = std::shared_ptr<Set>;
using ConstSetPtr = std::shared_ptr<const Set>;
using Sets = std::vector<SetPtr>;

class IFunction;
using FunctionPtr = std::shared_ptr<IFunction>;

/// Class for mayBeTrueInRange function.
class MergeTreeSetIndex
{
public:
    /** Mapping for tuple positions from Set::set_elements to
      * position of pk index and data type of this pk column
      * and functions chain applied to this column.
      */
    struct PKTuplePositionMapping
    {
        size_t tuple_index;
        size_t pk_index;
        std::vector<FunctionBasePtr> functions;
    };

    MergeTreeSetIndex(const SetElements & set_elements, std::vector<PKTuplePositionMapping> && indexes_mapping_);

    BoolMask mayBeTrueInRange(const std::vector<Range> & key_ranges, const DataTypes & data_types);
private:
    using OrderedTuples = std::vector<std::vector<FieldWithInfinity>>;
    OrderedTuples ordered_set;

    std::vector<PKTuplePositionMapping> indexes_mapping;
};

 }
