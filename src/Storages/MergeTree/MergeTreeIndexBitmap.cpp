#include <Storages/MergeTree/MergeTreeIndexBitmap.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/Set.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/quoteString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
}


// ============================================================================
// MergeTreeIndexGranuleBitmap Implementation
// ============================================================================

MergeTreeIndexGranuleBitmap::MergeTreeIndexGranuleBitmap(
    const String & index_name_,
    const Block & index_sample_block_,
    size_t max_cardinality_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , max_cardinality(max_cardinality_)
{
}

MergeTreeIndexGranuleBitmap::MergeTreeIndexGranuleBitmap(
    const String & index_name_,
    const Block & index_sample_block_,
    size_t max_cardinality_,
    std::map<Field, std::unique_ptr<RoaringBitmapWithSmallSet<UInt32, 32>>> && bitmaps_,
    std::unique_ptr<RoaringBitmapWithSmallSet<UInt32, 32>> && null_bitmap_,
    size_t total_rows_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , max_cardinality(max_cardinality_)
    , bitmaps(std::move(bitmaps_))
    , null_bitmap(std::move(null_bitmap_))
    , total_rows(total_rows_)
{
}

void MergeTreeIndexGranuleBitmap::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to write empty bitmap index {}", backQuote(index_name));

    /// Write total rows
    writeVarUInt(total_rows, ostr);

    /// Write number of distinct values
    writeVarUInt(bitmaps.size(), ostr);

    /// Write each value and its bitmap
    const DataTypePtr & type = index_sample_block.getByPosition(0).type;
    auto serialization = type->getDefaultSerialization();

    for (const auto & [value, bitmap_ptr] : bitmaps)
    {
        /// Serialize the value
        serialization->serializeBinary(value, ostr, {});

        /// Serialize the bitmap
        WriteBufferFromOwnString bitmap_buf;
        const_cast<RoaringBitmapWithSmallSet<UInt32, 32> *>(bitmap_ptr.get())->write(bitmap_buf);
        writeStringBinary(bitmap_buf.str(), ostr);
    }

    /// Write NULL bitmap
    WriteBufferFromOwnString null_bitmap_buf;
    if (null_bitmap)
        const_cast<RoaringBitmapWithSmallSet<UInt32, 32> *>(null_bitmap.get())->write(null_bitmap_buf);
    writeStringBinary(null_bitmap_buf.str(), ostr);
}

void MergeTreeIndexGranuleBitmap::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    bitmaps.clear();
    null_bitmap.reset();

    /// Read total rows
    readVarUInt(total_rows, istr);

    /// Read number of distinct values
    size_t num_values;
    readVarUInt(num_values, istr);

    if (num_values > max_cardinality)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Bitmap index cardinality {} exceeds maximum {}", num_values, max_cardinality);

    /// Read each value and its bitmap
    const DataTypePtr & type = index_sample_block.getByPosition(0).type;
    auto serialization = type->getDefaultSerialization();
    FormatSettings format_settings;

    for (size_t i = 0; i < num_values; ++i)
    {
        /// Deserialize the value
        Field value;
        serialization->deserializeBinary(value, istr, format_settings);

        /// Deserialize the bitmap
        String bitmap_str;
        readStringBinary(bitmap_str, istr);
        ReadBufferFromString bitmap_buf(bitmap_str);

        auto bitmap = std::make_unique<RoaringBitmapWithSmallSet<UInt32, 32>>();
        bitmap->read(bitmap_buf);

        bitmaps[value] = std::move(bitmap);
    }

    /// Read NULL bitmap
    String null_bitmap_str;
    readStringBinary(null_bitmap_str, istr);
    if (!null_bitmap_str.empty())
    {
        ReadBufferFromString null_bitmap_buf(null_bitmap_str);
        null_bitmap = std::make_unique<RoaringBitmapWithSmallSet<UInt32, 32>>();
        null_bitmap->read(null_bitmap_buf);
    }
}

size_t MergeTreeIndexGranuleBitmap::memoryUsageBytes() const
{
    size_t total = sizeof(*this);

    /// Estimate bitmap memory usage
    for (const auto & [value, bitmap_ptr] : bitmaps)
    {
        total += 64;  /// Approximate Field size
        if (bitmap_ptr)
            total += bitmap_ptr->size() * sizeof(UInt32);  /// Approximate bitmap size
    }

    if (null_bitmap)
        total += null_bitmap->size() * sizeof(UInt32);

    return total;
}


// ============================================================================
// MergeTreeIndexAggregatorBitmap Implementation
// ============================================================================

MergeTreeIndexAggregatorBitmap::MergeTreeIndexAggregatorBitmap(
    const String & index_name_,
    const Block & index_sample_block_,
    size_t max_cardinality_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , max_cardinality(max_cardinality_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorBitmap::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleBitmap>(
        index_name,
        index_sample_block,
        max_cardinality,
        std::move(bitmaps),
        std::move(null_bitmap),
        total_rows);

    bitmaps.clear();
    null_bitmap.reset();
    total_rows = 0;
    current_row_offset = 0;

    return granule;
}

void MergeTreeIndexAggregatorBitmap::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The provided position is not less than the number of block rows. Position: {}, Block rows: {}.",
            *pos, block.rows());

    size_t rows_read = std::min(limit, block.rows() - *pos);

    /// Get the indexed column
    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column_with_type = block.getByName(index_column_name);
    const auto & column = column_with_type.column;

    /// Handle nullable columns
    const ColumnNullable * column_nullable = typeid_cast<const ColumnNullable *>(column.get());
    const IColumn * data_column = column_nullable ? &column_nullable->getNestedColumn() : column.get();

    /// Process each row
    for (size_t i = 0; i < rows_read; ++i)
    {
        size_t row_idx = *pos + i;
        UInt32 bitmap_pos = current_row_offset + static_cast<UInt32>(i);

        /// Check for NULL
        if (column_nullable && column_nullable->isNullAt(row_idx))
        {
            if (!null_bitmap)
                null_bitmap = std::make_unique<RoaringBitmapWithSmallSet<UInt32, 32>>();
            null_bitmap->add(bitmap_pos);
        }
        else
        {
            /// Get the value
            Field value;
            data_column->get(row_idx, value);

            /// Check cardinality limit
            if (bitmaps.size() >= max_cardinality && bitmaps.find(value) == bitmaps.end())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Bitmap index cardinality limit {} exceeded for index {}",
                    max_cardinality, backQuote(index_name));
            }

            /// Add to bitmap (create if doesn't exist)
            auto & bitmap_ptr = bitmaps[value];
            if (!bitmap_ptr)
                bitmap_ptr = std::make_unique<RoaringBitmapWithSmallSet<UInt32, 32>>();
            bitmap_ptr->add(bitmap_pos);
        }
    }

    total_rows += rows_read;
    current_row_offset += static_cast<UInt32>(rows_read);
    *pos += rows_read;
}


// ============================================================================
// MergeTreeIndexConditionBitmap Implementation
// ============================================================================

namespace
{

KeyCondition buildCondition(const IndexDescription & index, const ActionsDAGWithInversionPushDown & filter_dag, ContextPtr context)
{
    return KeyCondition{filter_dag, context, index.column_names, index.expression};
}

}

MergeTreeIndexConditionBitmap::MergeTreeIndexConditionBitmap(
    const IndexDescription & index,
    const ActionsDAGWithInversionPushDown & filter_dag,
    ContextPtr context)
    : index_data_type(index.data_types[0])
    , condition(buildCondition(index, filter_dag, context))
{
}

bool MergeTreeIndexConditionBitmap::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue();
}

bool MergeTreeIndexConditionBitmap::checkInBitmap(
    const MergeTreeIndexGranuleBitmap & granule,
    const KeyCondition::RPNElement & element) const
{
    /// Handle different RPN element types
    if (element.function == KeyCondition::RPNElement::FUNCTION_IN_RANGE ||
        element.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE)
    {
        /// For equality check (range with same left and right bounds)
        if (element.range.left == element.range.right)
        {
            const Field & value = element.range.left;
            bool found = false;

            if (value.isNull())
                found = granule.null_bitmap && granule.null_bitmap->size() > 0;
            else
                found = granule.bitmaps.find(value) != granule.bitmaps.end();

            if (element.function == KeyCondition::RPNElement::FUNCTION_IN_RANGE)
                return found;
            else
                return !found || !granule.bitmaps.empty() || (granule.null_bitmap && granule.null_bitmap->size() > 0);
        }

        /// For range queries, check if any value in the granule falls within the range
        bool found_in_range = false;
        for (const auto & [value, bitmap_ptr] : granule.bitmaps)
        {
            (void)bitmap_ptr;
            if (element.range.contains(value))
            {
                found_in_range = true;
                break;
            }
        }

        if (element.function == KeyCondition::RPNElement::FUNCTION_IN_RANGE)
            return found_in_range;
        else
            return !found_in_range || !granule.bitmaps.empty();
    }
    else if (element.function == KeyCondition::RPNElement::FUNCTION_IN_SET)
    {
        /// For IN predicates with a set, check if any value in the set exists in the granule
        if (!element.set_index)
            return true; /// Conservative: no set index available

        /// Get the ordered set of values
        const auto & values = element.set_index->getOrderedSet();
        if (values.empty() || element.set_index->size() == 0)
            return false; /// Empty set means no matches possible

        /// Check if any value from the IN set exists in this granule's bitmaps
        for (size_t i = 0; i < element.set_index->size(); ++i)
        {
            Field value;
            values[0]->get(i, value);

            /// Check for NULL
            if (value.isNull())
            {
                if (granule.null_bitmap && granule.null_bitmap->size() > 0)
                    return true; /// Found NULL in granule
            }
            else
            {
                /// Check if this value exists in the granule
                if (granule.bitmaps.find(value) != granule.bitmaps.end())
                    return true; /// Found at least one matching value
            }
        }

        /// None of the values in the IN set exist in this granule - can skip it!
        return false;
    }
    else if (element.function == KeyCondition::RPNElement::FUNCTION_NOT_IN_SET)
    {
        /// For NOT IN predicates, check if ALL values in the granule are in the exclusion set
        /// If the granule contains ONLY values from the NOT IN set, we can skip it
        /// Otherwise, we must read it (conservative approach)

        if (!element.set_index)
            return true; /// Conservative: no set index available

        if (element.set_index->size() == 0)
            return true; /// Empty NOT IN set means all values match

        /// If granule is empty, skip it
        if (granule.bitmaps.empty() && (!granule.null_bitmap || granule.null_bitmap->size() == 0))
            return false;

        /// Conservative approach: we can only skip if we're certain the granule has no matching rows
        /// This is hard to determine without reading the data, so return true
        /// A more aggressive optimization would be to check if all granule values are in the set,
        /// but this requires iterating all granule values which may be expensive
        return true;
    }
    else if (element.function == KeyCondition::RPNElement::FUNCTION_IS_NULL)
    {
        return granule.null_bitmap && granule.null_bitmap->size() > 0;
    }
    else if (element.function == KeyCondition::RPNElement::FUNCTION_IS_NOT_NULL)
    {
        /// If there are any non-null values, return true
        return !granule.bitmaps.empty();
    }

    /// For other functions, conservatively return true
    return true;
}

bool MergeTreeIndexConditionBitmap::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    const auto & granule = typeid_cast<const MergeTreeIndexGranuleBitmap &>(*idx_granule);

    /// Evaluate the RPN expression
    const auto & rpn = condition.getRPN();
    std::vector<bool> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == KeyCondition::RPNElement::FUNCTION_UNKNOWN ||
            element.function == KeyCondition::RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (element.function == KeyCondition::RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.push_back(false);
        }
        else if (element.function == KeyCondition::RPNElement::FUNCTION_NOT)
        {
            if (!rpn_stack.empty())
            {
                bool val = rpn_stack.back();
                rpn_stack.pop_back();
                rpn_stack.push_back(!val);
            }
        }
        else if (element.function == KeyCondition::RPNElement::FUNCTION_AND)
        {
            if (rpn_stack.size() >= 2)
            {
                bool val2 = rpn_stack.back(); rpn_stack.pop_back();
                bool val1 = rpn_stack.back(); rpn_stack.pop_back();
                rpn_stack.push_back(val1 && val2);
            }
        }
        else if (element.function == KeyCondition::RPNElement::FUNCTION_OR)
        {
            if (rpn_stack.size() >= 2)
            {
                bool val2 = rpn_stack.back(); rpn_stack.pop_back();
                bool val1 = rpn_stack.back(); rpn_stack.pop_back();
                rpn_stack.push_back(val1 || val2);
            }
        }
        else
        {
            /// Check if this condition matches the bitmap
            rpn_stack.push_back(checkInBitmap(granule, element));
        }
    }

    /// If stack is empty or has true, we may need to read the granule
    return rpn_stack.empty() || rpn_stack.back();
}


// ============================================================================
// MergeTreeIndexBitmap Implementation
// ============================================================================

MergeTreeIndexGranulePtr MergeTreeIndexBitmap::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleBitmap>(index.name, index.sample_block, max_cardinality);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexBitmap::createIndexAggregator(const MergeTreeWriterSettings & /*settings*/) const
{
    return std::make_shared<MergeTreeIndexAggregatorBitmap>(index.name, index.sample_block, max_cardinality);
}

MergeTreeIndexConditionPtr MergeTreeIndexBitmap::createIndexCondition(
    const ActionsDAG::Node * predicate, ContextPtr context) const
{
    ActionsDAGWithInversionPushDown filter_dag(predicate, context);
    return std::make_shared<MergeTreeIndexConditionBitmap>(index, filter_dag, context);
}

MergeTreeIndexFormat MergeTreeIndexBitmap::getDeserializedFormat(
    const IDataPartStorage & data_part_storage,
    const std::string & relative_path_prefix) const
{
    if (data_part_storage.existsFile(relative_path_prefix + ".idx2"))
        return {2, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx2"}}};
    if (data_part_storage.existsFile(relative_path_prefix + ".idx"))
        return {1, {{MergeTreeIndexSubstream::Type::Regular, "", ".idx"}}};
    return {0 /* unknown */, {}};
}


// ============================================================================
// Factory Functions
// ============================================================================

MergeTreeIndexPtr bitmapIndexCreator(const IndexDescription & index)
{
    /// Parse max_cardinality parameter
    size_t max_cardinality = 10000;  /// Default value

    if (!index.arguments.empty())
    {
        if (index.arguments.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Bitmap index requires exactly one argument (max_cardinality), got {}",
                index.arguments.size());

        const auto & argument = index.arguments[0];
        if (argument.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Bitmap index max_cardinality argument must be a positive integer");

        max_cardinality = argument.safeGet<UInt64>();

        if (max_cardinality == 0 || max_cardinality > 1000000)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Bitmap index max_cardinality must be between 1 and 1000000, got {}",
                max_cardinality);
    }

    return std::make_shared<MergeTreeIndexBitmap>(index, max_cardinality);
}

void bitmapIndexValidator(const IndexDescription & index, bool attach)
{
    if (attach)
        return;

    /// Bitmap index only supports single column
    if (index.column_names.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Bitmap index supports only one column, got {}", index.column_names.size());

    /// Check that the column type is supported
    const auto & column = index.sample_block.getByPosition(0);
    const auto & type = column.type;

    /// Bitmap index works best with discrete types
    /// We support: integers, strings, dates, enums, but not floats or complex types
    if (type->isValueRepresentedByNumber() && !type->isValueRepresentedByInteger())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Bitmap index does not support floating point types for column {}", column.name);
    }

    if (isDynamic(type) || isVariant(type))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "{} data type of column {} is not allowed in bitmap index because the column of that type can contain values with different data types",
            type->getName(), column.name);
    }

    /// Validate max_cardinality parameter
    if (!index.arguments.empty())
    {
        if (index.arguments.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Bitmap index requires exactly one argument (max_cardinality), got {}",
                index.arguments.size());

        const auto & argument = index.arguments[0];
        if (argument.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Bitmap index max_cardinality argument must be a positive integer");

        size_t max_cardinality = argument.safeGet<UInt64>();
        if (max_cardinality == 0 || max_cardinality > 1000000)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Bitmap index max_cardinality must be between 1 and 1000000, got {}",
                max_cardinality);
    }
}

}

