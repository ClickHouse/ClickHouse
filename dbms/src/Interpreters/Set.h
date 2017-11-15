#pragma once

#include <shared_mutex>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Limits.h>
#include <Interpreters/SetVariants.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/BoolMask.h>

#include <common/logger_useful.h>


namespace DB
{

struct Range;


/** Data structure for implementation of IN expression.
  */
class Set
{
public:
    Set(const Limits & limits) :
        log(&Logger::get("Set")),
        max_rows(limits.max_rows_in_set),
        max_bytes(limits.max_bytes_in_set),
        overflow_mode(limits.set_overflow_mode)
    {
    }

    bool empty() const { return data.empty(); }

    /** Create a Set from expression (specified literally in the query).
      * 'types' - types of what are on the left hand side of IN.
      * 'node' - list of values: 1, 2, 3 or list of tuples: (1, 2), (3, 4), (5, 6).
      * 'create_ordered_set' - if true, create ordered vector of elements. For primary key to work.
      */
    void createFromAST(const DataTypes & types, ASTPtr node, const Context & context, bool create_ordered_set);

    // Returns false, if some limit was exceeded and no need to insert more data.
    bool insertFromBlock(const Block & block, bool create_ordered_set = false);

    /** For columns of 'block', check belonging of corresponding rows to the set.
      * Return UInt8 column with the result.
      */
    ColumnPtr execute(const Block & block, bool negative) const;

    std::string describe() const;

    /// Check, if the Set could possibly contain elements for specified range.
    BoolMask mayBeTrueInRange(const Range & range) const;

    size_t getTotalRowCount() const { return data.getTotalRowCount(); }
    size_t getTotalByteCount() const { return data.getTotalByteCount(); }

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
    void executeArray(const ColumnArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const;

    /// If in the left part columns contains the same types as the elements of the set.
    void executeOrdinary(
        const ConstColumnPlainPtrs & key_columns,
        ColumnUInt8::Container_t & vec_res,
        bool negative,
        const PaddedPODArray<UInt8> * null_map) const;

    /// Check whether the permissible sizes of keys set reached
    bool checkSetSizeLimits() const;

    /// Vector of ordered elements of `Set`.
    /// It is necessary for the index to work on the primary key in the IN statement.
    using OrderedSetElements = std::vector<Field>;
    using OrderedSetElementsPtr = std::unique_ptr<OrderedSetElements>;
    OrderedSetElementsPtr ordered_set_elements;

    /** Protects work with the set in the functions `insertFromBlock` and `execute`.
      * These functions can be called simultaneously from different threads only when using StorageSet,
      *  and StorageSet calls only these two functions.
      * Therefore, the rest of the functions for working with set are not protected.
      */
    mutable std::shared_mutex rwlock;


    template <typename Method>
    void insertFromBlockImpl(
        Method & method,
        const ConstColumnPlainPtrs & key_columns,
        size_t rows,
        SetVariants & variants,
        ConstNullMapPtr null_map);

    template <typename Method, bool has_null_map>
    void insertFromBlockImplCase(
        Method & method,
        const ConstColumnPlainPtrs & key_columns,
        size_t rows,
        SetVariants & variants,
        ConstNullMapPtr null_map);

    template <typename Method>
    void executeImpl(
        Method & method,
        const ConstColumnPlainPtrs & key_columns,
        ColumnUInt8::Container_t & vec_res,
        bool negative,
        size_t rows,
        ConstNullMapPtr null_map) const;

    template <typename Method, bool has_null_map>
    void executeImplCase(
        Method & method,
        const ConstColumnPlainPtrs & key_columns,
        ColumnUInt8::Container_t & vec_res,
        bool negative,
        size_t rows,
        ConstNullMapPtr null_map) const;

    template <typename Method>
    void executeArrayImpl(
        Method & method,
        const ConstColumnPlainPtrs & key_columns,
        const ColumnArray::Offsets_t & offsets,
        ColumnUInt8::Container_t & vec_res,
        bool negative,
        size_t rows) const;
};

using SetPtr = std::shared_ptr<Set>;
using ConstSetPtr = std::shared_ptr<const Set>;
using Sets = std::vector<SetPtr>;

}
