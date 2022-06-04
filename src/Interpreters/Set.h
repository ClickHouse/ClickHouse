#pragma once

#include <shared_mutex>
#include <Core/Block.h>
#include <QueryPipeline/SizeLimits.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/SetVariants.h>
#include <Interpreters/BloomFilter.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/BoolMask.h>

#include <Common/logger_useful.h>


namespace DB
{

struct Range;

class Context;
class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

/** Data structure for implementation of IN expression. */
class ISet
{
public:
    /// 'fill_set_elements': in addition to hash table
    /// (that is useful only for checking that some value is in the set and may not store the original values),
    /// store all set elements in explicit form.
    /// This is needed for subsequent use for index.
    virtual ~ISet() {}

    virtual bool empty() const = 0;

    /** Set can be created either from AST or from a stream of data (subquery result).
      */

    /** Create a Set from stream.
      * Call setHeader, then call insertFromBlock for each block.
      */
    virtual void setHeader(const Block & header) = 0;

    /// Returns false, if some limit was exceeded and no need to insert more data.
    virtual bool insertFromBlock(const Block & block) = 0;
    /// Call after all blocks were inserted. To get the information that set is already created.
    virtual void finishInsert() = 0;

    /** For columns of 'block', check belonging of corresponding rows to the set.
      * Return UInt8 column with the result.
      */
    virtual ColumnPtr execute(const Block & block, bool negative) const = 0;
};

using ISetPtr = std::shared_ptr<ISet>;
using IConstSetPtr = std::shared_ptr<const ISet>;
using ISets = std::vector<ISetPtr>;


class BloomFilterSet : public ISet
{
public:
    BloomFilterSet(const SizeLimits & limits_, bool fill_set_elements_, bool transform_null_in_)
        : limits(limits_), fill_set_elements(fill_set_elements_), transform_null_in(transform_null_in_)
    {
    }

    virtual bool empty() const override { return data.isEmpty(); }

    /** Set can be created either from AST or from a stream of data (subquery result).
      */

    /** Create a Set from stream.
      * Call setHeader, then call insertFromBlock for each block.
      */
    void setHeader(const Block & header) override;

    /// Returns false, if some limit was exceeded and no need to insert more data.
    virtual bool insertFromBlock(const Block & block) override;
    /// Call after all blocks were inserted. To get the information that set is already created.
    virtual void finishInsert() override { is_created = true; }

    bool isCreated() const { return is_created; }

    /** For columns of 'block', check belonging of corresponding rows to the set.
      * Return UInt8 column with the result.
      */
    virtual ColumnPtr execute(const Block & block, bool negative) const override;

    size_t getTotalRowCount() const { return data.numAdded(); }
    size_t getTotalByteCount() const { return data.getFilter().size() * sizeof(uint64_t); }

    const DataTypes & getDataTypes() const { return data_types; }
    const DataTypes & getElementsTypes() const { return set_elements_types; }

    bool hasExplicitSetElements() const { return fill_set_elements; }
    Columns getSetElements() const { return { set_elements.begin(), set_elements.end() }; }

    void checkColumnsNumber(size_t num_key_columns) const;
    bool areTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const;
    void checkTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const;

private:
    size_t keys_size = 0;
    Sizes key_sizes;

    BloomFilter data{1024 * 10, 4, 42};

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

    /// Types for set_elements.
    DataTypes set_elements_types;

    /// Limitations on the maximum size of the set
    SizeLimits limits;

    /// Do we need to additionally store all elements of the set in explicit form for subsequent use for index.
    bool fill_set_elements;

    /// If true, insert NULL values to set.
    bool transform_null_in;

    /// Check if set contains all the data.
    bool is_created = false;

    /// If in the left part columns contains the same types as the elements of the set.
    void executeOrdinary(
            const ColumnRawPtrs & key_columns,
            ColumnUInt8::Container & vec_res,
            bool negative,
            const PaddedPODArray<UInt8> * null_map) const;

    /// Collected elements of `Set`.
    /// It is necessary for the index to work on the primary key in the IN statement.
    std::vector<IColumn::WrappedPtr> set_elements;

    /** Protects work with the set in the functions `insertFromBlock` and `execute`.
      * These functions can be called simultaneously from different threads only when using StorageSet,
      *  and StorageSet calls only these two functions.
      * Therefore, the rest of the functions for working with set are not protected.
      */
    mutable std::shared_mutex rwlock;

    void insertFromBlockImpl(
            const ColumnRawPtrs & key_columns,
            size_t rows,
            ConstNullMapPtr null_map,
            ColumnUInt8::Container * out_filter);

    template <bool has_null_map, bool build_filter>
    void insertFromBlockImplCase(
            const ColumnRawPtrs & key_columns,
            size_t rows,
            ConstNullMapPtr null_map,
            ColumnUInt8::Container * out_filter);

    void executeImpl(
            const ColumnRawPtrs & key_columns,
            ColumnUInt8::Container & vec_res,
            bool negative,
            size_t rows,
            ConstNullMapPtr null_map) const;

    template <bool has_null_map>
    void executeImplCase(
            const ColumnRawPtrs & key_columns,
            ColumnUInt8::Container & vec_res,
            bool negative,
            size_t rows,
            ConstNullMapPtr null_map) const;
};

/** Data structure for implementation of IN expression.
  */
class Set
{
public:
    /// 'fill_set_elements': in addition to hash table
    /// (that is useful only for checking that some value is in the set and may not store the original values),
    /// store all set elements in explicit form.
    /// This is needed for subsequent use for index.
    Set(const SizeLimits & limits_, bool fill_set_elements_, bool transform_null_in_)
        : log(&Poco::Logger::get("Set")),
        limits(limits_), fill_set_elements(fill_set_elements_), transform_null_in(transform_null_in_)
    {
    }

    /** Set can be created either from AST or from a stream of data (subquery result).
      */

    /** Create a Set from stream.
      * Call setHeader, then call insertFromBlock for each block.
      */
    void setHeader(const ColumnsWithTypeAndName & header);

    /// Returns false, if some limit was exceeded and no need to insert more data.
    bool insertFromBlock(const ColumnsWithTypeAndName & columns);
    /// Call after all blocks were inserted. To get the information that set is already created.
    void finishInsert() { is_created = true; }

    bool isCreated() const { return is_created; }

    /** For columns of 'block', check belonging of corresponding rows to the set.
      * Return UInt8 column with the result.
      */
    ColumnPtr execute(const ColumnsWithTypeAndName & columns, bool negative) const;

    bool empty() const;
    size_t getTotalRowCount() const;
    size_t getTotalByteCount() const;

    const DataTypes & getDataTypes() const { return data_types; }
    const DataTypes & getElementsTypes() const { return set_elements_types; }

    bool hasExplicitSetElements() const { return fill_set_elements; }
    Columns getSetElements() const { return { set_elements.begin(), set_elements.end() }; }

    void checkColumnsNumber(size_t num_key_columns) const;
    bool areTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const;
    void checkTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const;

private:
    size_t keys_size = 0;
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

    /// Types for set_elements.
    DataTypes set_elements_types;

    Poco::Logger * log;

    /// Limitations on the maximum size of the set
    SizeLimits limits;

    /// Do we need to additionally store all elements of the set in explicit form for subsequent use for index.
    bool fill_set_elements;

    /// If true, insert NULL values to set.
    bool transform_null_in;

    /// Check if set contains all the data.
    bool is_created = false;

    /// If in the left part columns contains the same types as the elements of the set.
    void executeOrdinary(
        const ColumnRawPtrs & key_columns,
        ColumnUInt8::Container & vec_res,
        bool negative,
        const PaddedPODArray<UInt8> * null_map) const;

    /// Collected elements of `Set`.
    /// It is necessary for the index to work on the primary key in the IN statement.
    std::vector<IColumn::WrappedPtr> set_elements;

    /** Protects work with the set in the functions `insertFromBlock` and `execute`.
      * These functions can be called simultaneously from different threads only when using StorageSet,
      */
    mutable std::shared_mutex rwlock;

    template <typename Method>
    void insertFromBlockImpl(
        Method & method,
        const ColumnRawPtrs & key_columns,
        size_t rows,
        SetVariants & variants,
        ConstNullMapPtr null_map,
        ColumnUInt8::Container * out_filter);

    template <typename Method, bool has_null_map, bool build_filter>
    void insertFromBlockImplCase(
        Method & method,
        const ColumnRawPtrs & key_columns,
        size_t rows,
        SetVariants & variants,
        ConstNullMapPtr null_map,
        ColumnUInt8::Container * out_filter);

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
};

using SetPtr = std::shared_ptr<Set>;
using ConstSetPtr = std::shared_ptr<const Set>;
using Sets = std::vector<SetPtr>;


class IFunction;
using FunctionPtr = std::shared_ptr<IFunction>;

/** Class that represents single value with possible infinities.
  * Single field is stored in column for more optimal inplace comparisons with other regular columns.
  * Extracting fields from columns and further their comparison is suboptimal and requires extra copying.
  */
struct FieldValue
{
    FieldValue(MutableColumnPtr && column_) : column(std::move(column_)) {}
    void update(const Field & x);

    bool isNormal() const { return !value.isPositiveInfinity() && !value.isNegativeInfinity(); }
    bool isPositiveInfinity() const { return value.isPositiveInfinity(); }
    bool isNegativeInfinity() const { return value.isNegativeInfinity(); }

    Field value; // Null, -Inf, +Inf

    // If value is Null, uses the actual value in column
    MutableColumnPtr column;
};


/// Class for checkInRange function.
class MergeTreeSetIndex
{
public:
    /** Mapping for tuple positions from Set::set_elements to
      * position of pk index and functions chain applied to this column.
      */
    struct KeyTuplePositionMapping
    {
        size_t tuple_index;
        size_t key_index;
        std::vector<FunctionBasePtr> functions;
    };

    MergeTreeSetIndex(const Columns & set_elements, std::vector<KeyTuplePositionMapping> && indexes_mapping_);

    size_t size() const { return ordered_set.at(0)->size(); }

    bool hasMonotonicFunctionsChain() const;

    BoolMask checkInRange(const std::vector<Range> & key_ranges, const DataTypes & data_types, bool single_point = false) const;

private:
    // If all arguments in tuple are key columns, we can optimize NOT IN when there is only one element.
    bool has_all_keys;
    Columns ordered_set;
    std::vector<KeyTuplePositionMapping> indexes_mapping;

    using FieldValues = std::vector<FieldValue>;
};

}
