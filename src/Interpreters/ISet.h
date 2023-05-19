#pragma once

#include <Core/Block.h>
#include <QueryPipeline/SizeLimits.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/SetVariants.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/BoolMask.h>

#include <Common/SharedMutex.h>


namespace DB
{

class ISetHelper {
    
};


struct Range;

class Context;
class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;

class Chunk;

/** Data structure for implementation of IN expression.
  */
class ISet
{
friend class ISetHelper;

public:
    /// 'fill_set_elements': in addition to hash table
    /// (that is useful only for checking that some value is in the set and may not store the original values),
    /// store all set elements in explicit form.
    /// This is needed for subsequent use for index.
    ISet(const SizeLimits & limits_, bool fill_set_elements_, bool transform_null_in_)
        : log(&Poco::Logger::get("ISet")),
        limits(limits_), fill_set_elements(fill_set_elements_), transform_null_in(transform_null_in_)
    {
    }


    virtual ~ISet() {}

    /** Set can be created either from AST or from a stream of data (subquery result).
      */

    /** Create a Set from stream.
      * Call setHeader, then call insertFromBlock for each block.
      */
    virtual void setHeader(const ColumnsWithTypeAndName & header);

    /// Returns false, if some limit was exceeded and no need to insert more data.
    virtual bool insertFromBlock(const Columns & columns);
    virtual bool insertFromBlock(const ColumnsWithTypeAndName & columns);

    /// Call after all blocks were inserted. To get the information that set is already created.
    virtual void finishInsert() { is_created = true; }

    /// finishInsert and isCreated are thread-safe
    virtual bool isCreated() const { return is_created.load(); }

    virtual void checkIsCreated() const;

    /** For columns of 'block', check belonging of corresponding rows to the set.
      * Return UInt8 column with the result.
      */
    virtual ColumnPtr execute(const ColumnsWithTypeAndName & columns, bool negative) const;

    virtual bool empty() const;
    virtual size_t getTotalRowCount() const;
    virtual size_t getTotalByteCount() const;

    virtual const DataTypes & getDataTypes() const { return data_types; }
    virtual const DataTypes & getElementsTypes() const { return set_elements_types; }

    virtual bool hasExplicitSetElements() const { return fill_set_elements; }
    virtual Columns getSetElements() const { checkIsCreated(); return { set_elements.begin(), set_elements.end() }; }

    virtual void checkColumnsNumber(size_t num_key_columns) const;
    virtual bool areTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const;
    virtual void checkTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const;

protected:
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
    std::atomic<bool> is_created = false;

    virtual void initialize_data(ColumnRawPtrs key_columns) = 0;

    /// If in the left part columns contains the same types as the elements of the set.
    virtual void executeOrdinary(
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
    mutable SharedMutex rwlock;

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

using SetPtr = std::shared_ptr<ISet>;
using ConstSetPtr = std::shared_ptr<const ISet>;
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
