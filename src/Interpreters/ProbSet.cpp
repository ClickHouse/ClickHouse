#include <optional>

#include <Core/Field.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>

#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Interpreters/ProbSet.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/sortBlock.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>

#include <Processors/Chunk.h>

#include <Storages/MergeTree/KeyCondition.h>

#include <base/range.h>
#include <base/sort.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


// template <typename Method>
// void NO_INLINE Set::insertFromBlockImpl(
//     Method & method,
//     const ColumnRawPtrs & key_columns,
//     size_t rows,
//     SetVariants & variants,
//     ConstNullMapPtr null_map,
//     ColumnUInt8::Container * out_filter)
// {
//     return;
// }


// template <typename Method, bool has_null_map, bool build_filter>
// void NO_INLINE Set::insertFromBlockImplCase(
//     Method & method,
//     const ColumnRawPtrs & key_columns,
//     size_t rows,
//     SetVariants & variants,
//     [[maybe_unused]] ConstNullMapPtr null_map,
//     [[maybe_unused]] ColumnUInt8::Container * out_filter)
// {
//     return;
// }


void ProbSet::setHeader([[maybe_unused]] const ColumnsWithTypeAndName & header)
{
    return;
}

bool ProbSet::insertFromBlock([[maybe_unused]] const ColumnsWithTypeAndName & columns)
{
    // Columns cols;
    // cols.reserve(columns.size());
    // for (const auto & column : columns)
    //     cols.emplace_back(column.column);
    // return insertFromBlock(cols);
    return true;
}

bool ProbSet::insertFromBlock([[maybe_unused]] const Columns & columns)
{
    return true;
}


ColumnPtr ProbSet::execute([[maybe_unused]] const ColumnsWithTypeAndName & columns, bool negative) const
{
    size_t num_key_columns = columns.size();

    if (0 == num_key_columns)
        throw Exception("Logical error: no columns passed to Set::execute method.", ErrorCodes::LOGICAL_ERROR);

    auto res = ColumnUInt8::create();
    ColumnUInt8::Container & vec_res = res->getData();
    vec_res.resize(columns.at(0).column->size());

    if (vec_res.empty())
        return res;

    std::shared_lock lock(rwlock);

    /// If the set is empty.
    
    if (negative)
        memset(vec_res.data(), 1, vec_res.size());
    else
        memset(vec_res.data(), 0, vec_res.size());
    return res;
    
}


bool ProbSet::empty() const
{
    std::shared_lock lock(rwlock);
    return data.empty();
}

size_t ProbSet::getTotalRowCount() const
{
    std::shared_lock lock(rwlock);
    return data.getTotalRowCount();
}

size_t ProbSet::getTotalByteCount() const
{
    std::shared_lock lock(rwlock);
    return data.getTotalByteCount();
}


// template <typename Method>
// void NO_INLINE Set::executeImpl(
//     Method & method,
//     const ColumnRawPtrs & key_columns,
//     ColumnUInt8::Container & vec_res,
//     bool negative,
//     size_t rows,
//     ConstNullMapPtr null_map) const
// {
//     return;
// }


// template <typename Method, bool has_null_map>
// void NO_INLINE Set::executeImplCase(
//     Method & method,
//     const ColumnRawPtrs & key_columns,
//     ColumnUInt8::Container & vec_res,
//     bool negative,
//     size_t rows,
//     ConstNullMapPtr null_map) const
// {
//    return;
// }


// void Set::executeOrdinary(
//     const ColumnRawPtrs & key_columns,
//     ColumnUInt8::Container & vec_res,
//     bool negative,
//     ConstNullMapPtr null_map) const
// {
//     return;
// }

void ProbSet::checkColumnsNumber(size_t num_key_columns) const
{
    if (data_types.size() != num_key_columns)
    {
        throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
                        "Number of columns in section IN doesn't match. {} at left, {} at right.",
                        num_key_columns, data_types.size());
    }
}

bool ProbSet::areTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const
{
    /// Out-of-bound access can happen when same set expression built with different columns.
    /// Caller may call this method to make sure that the set is indeed the one they want
    /// without awaring data_types.size().
    if (set_type_idx >= data_types.size())
        return false;
    return removeNullable(recursiveRemoveLowCardinality(data_types[set_type_idx]))
        ->equals(*removeNullable(recursiveRemoveLowCardinality(other_type)));
}

void ProbSet::checkTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const
{
    if (!this->areTypesEqual(set_type_idx, other_type))
        throw Exception("Types of column " + toString(set_type_idx + 1) + " in section IN don't match: "
                        + other_type->getName() + " on the left, "
                        + data_types[set_type_idx]->getName() + " on the right", ErrorCodes::TYPE_MISMATCH);
}

// MergeTreeSetIndex::MergeTreeSetIndex(const Columns & set_elements, std::vector<KeyTuplePositionMapping> && indexes_mapping_)
//     : has_all_keys(set_elements.size() == indexes_mapping_.size()), indexes_mapping(std::move(indexes_mapping_))
// {
//     ::sort(indexes_mapping.begin(), indexes_mapping.end(),
//         [](const KeyTuplePositionMapping & l, const KeyTuplePositionMapping & r)
//         {
//             return std::tie(l.key_index, l.tuple_index) < std::tie(r.key_index, r.tuple_index);
//         });

//     indexes_mapping.erase(std::unique(
//         indexes_mapping.begin(), indexes_mapping.end(),
//         [](const KeyTuplePositionMapping & l, const KeyTuplePositionMapping & r)
//         {
//             return l.key_index == r.key_index;
//         }), indexes_mapping.end());

//     size_t tuple_size = indexes_mapping.size();
//     ordered_set.resize(tuple_size);

//     for (size_t i = 0; i < tuple_size; ++i)
//         ordered_set[i] = set_elements[indexes_mapping[i].tuple_index];

//     Block block_to_sort;
//     SortDescription sort_description;
//     for (size_t i = 0; i < tuple_size; ++i)
//     {
//         String column_name = "_" + toString(i);
//         block_to_sort.insert({ordered_set[i], nullptr, column_name});
//         sort_description.emplace_back(column_name, 1, 1);
//     }

//     sortBlock(block_to_sort, sort_description);

//     for (size_t i = 0; i < tuple_size; ++i)
//         ordered_set[i] = block_to_sort.getByPosition(i).column;
// }


// /** Return the BoolMask where:
//   * 1: the intersection of the set and the range is non-empty
//   * 2: the range contains elements not in the set
//   */
// BoolMask MergeTreeSetIndex::checkInRange(const std::vector<Range> & key_ranges, const DataTypes & data_types, bool single_point) const
// {
//     size_t tuple_size = indexes_mapping.size();

//     FieldValues left_point;
//     FieldValues right_point;
//     left_point.reserve(tuple_size);
//     right_point.reserve(tuple_size);

//     for (size_t i = 0; i < tuple_size; ++i)
//     {
//         left_point.emplace_back(ordered_set[i]->cloneEmpty());
//         right_point.emplace_back(ordered_set[i]->cloneEmpty());
//     }

//     bool left_included = true;
//     bool right_included = true;

//     for (size_t i = 0; i < tuple_size; ++i)
//     {
//         std::optional<Range> new_range = KeyCondition::applyMonotonicFunctionsChainToRange(
//             key_ranges[indexes_mapping[i].key_index],
//             indexes_mapping[i].functions,
//             data_types[indexes_mapping[i].key_index],
//             single_point);

//         if (!new_range)
//             return {true, true};

//         left_point[i].update(new_range->left);
//         left_included &= new_range->left_included;
//         right_point[i].update(new_range->right);
//         right_included &= new_range->right_included;
//     }

//     /// lhs < rhs return -1
//     /// lhs == rhs return 0
//     /// lhs > rhs return 1
//     auto compare = [](const IColumn & lhs, const FieldValue & rhs, size_t row)
//     {
//         if (rhs.isNegativeInfinity())
//             return 1;
//         if (rhs.isPositiveInfinity())
//         {
//             Field f;
//             lhs.get(row, f);
//             if (f.isNull())
//                 return 0; // +Inf == +Inf
//             else
//                 return -1;
//         }
//         return lhs.compareAt(row, 0, *rhs.column, 1);
//     };

//     auto less = [this, &compare, tuple_size](size_t row, const auto & point)
//     {
//         for (size_t i = 0; i < tuple_size; ++i)
//         {
//             int res = compare(*ordered_set[i], point[i], row);
//             if (res)
//                 return res < 0;
//         }
//         return false;
//     };

//     auto equals = [this, &compare, tuple_size](size_t row, const auto & point)
//     {
//         for (size_t i = 0; i < tuple_size; ++i)
//             if (compare(*ordered_set[i], point[i], row) != 0)
//                 return false;
//         return true;
//     };

//     /** Because each hyperrectangle maps to a contiguous sequence of elements
//       * laid out in the lexicographically increasing order, the set intersects the range
//       * if and only if either bound coincides with an element or at least one element
//       * is between the lower bounds
//       */
//     auto indices = collections::range(0, size());
//     auto left_lower = std::lower_bound(indices.begin(), indices.end(), left_point, less);
//     auto right_lower = std::lower_bound(indices.begin(), indices.end(), right_point, less);

//     /// A special case of 1-element KeyRange. It's useful for partition pruning.
//     bool one_element_range = true;
//     for (size_t i = 0; i < tuple_size; ++i)
//     {
//         auto & left = left_point[i];
//         auto & right = right_point[i];
//         if (left.isNormal() && right.isNormal())
//         {
//             if (0 != left.column->compareAt(0, 0, *right.column, 1))
//             {
//                 one_element_range = false;
//                 break;
//             }
//         }
//         else if ((left.isPositiveInfinity() && right.isPositiveInfinity()) || (left.isNegativeInfinity() && right.isNegativeInfinity()))
//         {
//             /// Special value equality.
//         }
//         else
//         {
//             one_element_range = false;
//             break;
//         }
//     }
//     if (one_element_range && has_all_keys)
//     {
//         /// Here we know that there is one element in range.
//         /// The main difference with the normal case is that we can definitely say that
//         /// condition in this range is always TRUE (can_be_false = 0) or always FALSE (can_be_true = 0).

//         /// Check if it's an empty range
//         if (!left_included || !right_included)
//             return {false, true};
//         else if (left_lower != indices.end() && equals(*left_lower, left_point))
//             return {true, false};
//         else
//             return {false, true};
//     }

//     /// If there are more than one element in the range, it can always be false. Thus we only need to check if it may be true or not.
//     /// Given left_lower >= left_point, right_lower >= right_point, find if there may be a match in between left_lower and right_lower.
//     if (left_lower + 1 < right_lower)
//     {
//         /// There is an point in between: left_lower + 1
//         return {true, true};
//     }
//     else if (left_lower + 1 == right_lower)
//     {
//         /// Need to check if left_lower is a valid match, as left_point <= left_lower < right_point <= right_lower.
//         /// Note: left_lower is valid.
//         if (left_included || !equals(*left_lower, left_point))
//             return {true, true};

//         /// We are unlucky that left_point fails to cover a point. Now we need to check if right_point can cover right_lower.
//         /// Check if there is a match at the right boundary.
//         return {right_included && right_lower != indices.end() && equals(*right_lower, right_point), true};
//     }
//     else // left_lower == right_lower
//     {
//         /// Need to check if right_point is a valid match, as left_point < right_point <= left_lower = right_lower.
//         /// Check if there is a match at the left boundary.
//         return {right_included && right_lower != indices.end() && equals(*right_lower, right_point), true};
//     }
// }

// bool MergeTreeSetIndex::hasMonotonicFunctionsChain() const
// {
//     for (const auto & mapping : indexes_mapping)
//         if (!mapping.functions.empty())
//             return true;
//     return false;
// }

// void FieldValue::update(const Field & x)
// {
//     if (x.isNegativeInfinity() || x.isPositiveInfinity())
//         value = x;
//     else
//     {
//         /// Keep at most one element in column.
//         if (!column->empty())
//             column->popBack(1);
//         column->insert(x);
//         value = Field(); // Set back to normal value.
//     }
// }

}
