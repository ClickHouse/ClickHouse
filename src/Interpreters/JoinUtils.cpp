#include <Interpreters/JoinUtils.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/TableJoin.h>

#include <IO/WriteHelpers.h>

#include <Common/HashTable/Hash.h>
#include <Common/WeakHash.h>

#include <base/FnTraits.h>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
}

namespace
{

void insertFromNullableOrDefault(MutableColumnPtr & dst, const ColumnNullable * nullable_col)
{
    const auto & nested = nullable_col->getNestedColumn();
    const auto & nullmap = nullable_col->getNullMapColumn().getData();
    if (auto * lc = typeid_cast<ColumnLowCardinality *>(dst.get()); lc && !nested.lowCardinality())
    {
        for (size_t i = 0; i < nullable_col->size(); ++i)
        {
            if (nullmap[i])
                lc->insertDefault();
            else
                lc->insertRangeFromFullColumn(nested, i, 1);
        }
    }
    else
    {
        for (size_t i = 0; i < nullable_col->size(); ++i)
        {
            if (nullmap[i])
                dst->insertDefault();
            else
                dst->insertFrom(nested, i);
        }
    }
}

ColumnPtr changeLowCardinality(const ColumnPtr & column, const ColumnPtr & dst_sample)
{
    if (dst_sample->lowCardinality())
    {
        MutableColumnPtr lc = dst_sample->cloneEmpty();
        if (const auto * nullable_col = typeid_cast<const ColumnNullable *>(column.get()))
            insertFromNullableOrDefault(lc, nullable_col);
        else
            typeid_cast<ColumnLowCardinality &>(*lc).insertRangeFromFullColumn(*column, 0, column->size());
        return lc;
    }

    return column->convertToFullColumnIfLowCardinality();
}

struct LowcardAndNull
{
    bool is_lowcard;
    bool is_nullable;
};

LowcardAndNull getLowcardAndNullability(const ColumnPtr & col)
{
    if (col->lowCardinality())
    {
        /// Currently only `LowCardinality(Nullable(T))` is possible, but not `Nullable(LowCardinality(T))`
        assert(!col->canBeInsideNullable());
        const auto * col_as_lc = assert_cast<const ColumnLowCardinality *>(col.get());
        return {true, col_as_lc->nestedIsNullable()};
    }
    return {false, col->isNullable()};
}

}

namespace JoinCommon
{

void changeLowCardinalityInplace(ColumnWithTypeAndName & column)
{
    if (column.type->lowCardinality())
    {
        column.type = recursiveRemoveLowCardinality(column.type);
        column.column = column.column->convertToFullColumnIfLowCardinality();
    }
    else
    {
        column.type = std::make_shared<DataTypeLowCardinality>(column.type);
        MutableColumnPtr lc = column.type->createColumn();
        typeid_cast<ColumnLowCardinality &>(*lc).insertRangeFromFullColumn(*column.column, 0, column.column->size());
        column.column = std::move(lc);
    }
}

bool canBecomeNullable(const DataTypePtr & type)
{
    bool can_be_inside = type->canBeInsideNullable();
    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        can_be_inside |= low_cardinality_type->getDictionaryType()->canBeInsideNullable();
    return can_be_inside;
}

/// Add nullability to type.
/// Note: LowCardinality(T) transformed to LowCardinality(Nullable(T))
DataTypePtr convertTypeToNullable(const DataTypePtr & type)
{
    if (isNullableOrLowCardinalityNullable(type))
        return type;

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        const auto & dict_type = low_cardinality_type->getDictionaryType();
        if (dict_type->canBeInsideNullable())
            return std::make_shared<DataTypeLowCardinality>(makeNullable(dict_type));
    }

    if (type->canBeInsideNullable())
        return makeNullable(type);

    return type;
}

/// Convert column to nullable. If column LowCardinality or Const, convert nested column.
/// Returns nullptr if conversion cannot be performed.
static ColumnPtr tryConvertColumnToNullable(ColumnPtr col)
{
    if (col->isSparse())
        col = recursiveRemoveSparse(col);

    if (isColumnNullable(*col) || col->canBeInsideNullable())
        return makeNullable(col);

    if (col->lowCardinality())
    {
        const ColumnLowCardinality & col_lc = assert_cast<const ColumnLowCardinality &>(*col);
        if (col_lc.nestedIsNullable())
        {
            return col;
        }
        if (col_lc.nestedCanBeInsideNullable())
        {
            return col_lc.cloneNullable();
        }
    }
    else if (const ColumnConst * col_const = checkAndGetColumn<ColumnConst>(&*col))
    {
        const auto & nested = col_const->getDataColumnPtr();
        if (nested->isNullable() || nested->canBeInsideNullable())
        {
            return makeNullable(col);
        }
        if (nested->lowCardinality())
        {
            ColumnPtr nested_nullable = tryConvertColumnToNullable(nested);
            if (nested_nullable)
                return ColumnConst::create(nested_nullable, col_const->size());
        }
    }
    return nullptr;
}

void convertColumnToNullable(ColumnWithTypeAndName & column)
{
    if (!column.column)
    {
        column.type = convertTypeToNullable(column.type);
        return;
    }

    ColumnPtr nullable_column = tryConvertColumnToNullable(column.column);
    if (nullable_column)
    {
        column.type = convertTypeToNullable(column.type);
        column.column = std::move(nullable_column);
    }
}

void convertColumnsToNullable(Block & block, size_t starting_pos)
{
    for (size_t i = starting_pos; i < block.columns(); ++i)
        convertColumnToNullable(block.getByPosition(i));
}

void convertColumnsToNullable(MutableColumns & mutable_columns, size_t starting_pos)
{
    for (size_t i = starting_pos; i < mutable_columns.size(); ++i)
    {
        ColumnPtr column = std::move(mutable_columns[i]);
        column = makeNullable(column);
        mutable_columns[i] = IColumn::mutate(std::move(column));
    }
}

/// @warning It assumes that every NULL has default value in nested column (or it does not matter)
void removeColumnNullability(ColumnWithTypeAndName & column)
{
    if (column.type->lowCardinality())
    {
        /// LowCardinality(Nullable(T)) case
        const auto & dict_type = typeid_cast<const DataTypeLowCardinality *>(column.type.get())->getDictionaryType();
        column.type = std::make_shared<DataTypeLowCardinality>(removeNullable(dict_type));

        if (column.column && column.column->lowCardinality())
        {
            column.column = assert_cast<const ColumnLowCardinality *>(column.column.get())->cloneWithDefaultOnNull();
        }
    }
    else
    {
        column.type = removeNullable(column.type);

        if (column.column && column.column->isNullable())
        {
            column.column = column.column->convertToFullColumnIfConst();
            const auto * nullable_col = checkAndGetColumn<ColumnNullable>(column.column.get());
            if (!nullable_col)
            {
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Column '{}' is expected to be nullable", column.dumpStructure());
            }

            MutableColumnPtr mutable_column = nullable_col->getNestedColumn().cloneEmpty();
            insertFromNullableOrDefault(mutable_column, nullable_col);
            column.column = std::move(mutable_column);
        }
    }
}

/// Change both column nullability and low cardinality
void changeColumnRepresentation(const ColumnPtr & src_column, ColumnPtr & dst_column)
{
    bool nullable_src = src_column->isNullable();
    bool nullable_dst = dst_column->isNullable();

    ColumnPtr dst_not_null = JoinCommon::emptyNotNullableClone(dst_column);
    bool lowcard_src = JoinCommon::emptyNotNullableClone(src_column)->lowCardinality();
    bool lowcard_dst = dst_not_null->lowCardinality();
    bool change_lowcard = lowcard_src != lowcard_dst;

    if (nullable_src && !nullable_dst)
    {
        const auto & nullable = checkAndGetColumn<ColumnNullable>(*src_column);
        if (change_lowcard)
            dst_column = changeLowCardinality(nullable.getNestedColumnPtr(), dst_column);
        else
            dst_column = nullable.getNestedColumnPtr();
    }
    else if (!nullable_src && nullable_dst)
    {
        if (change_lowcard)
            dst_column = makeNullable(changeLowCardinality(src_column, dst_not_null));
        else
            dst_column = makeNullable(src_column);
    }
    else /// same nullability
    {
        if (change_lowcard)
        {
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*src_column))
            {
                dst_column = makeNullable(changeLowCardinality(nullable->getNestedColumnPtr(), dst_not_null));
                assert_cast<ColumnNullable &>(*dst_column->assumeMutable()).applyNullMap(nullable->getNullMapColumn());
            }
            else
                dst_column = changeLowCardinality(src_column, dst_not_null);
        }
        else
            dst_column = src_column;
    }
}

ColumnPtr emptyNotNullableClone(const ColumnPtr & column)
{
    if (column->isNullable())
        return checkAndGetColumn<ColumnNullable>(*column).getNestedColumnPtr()->cloneEmpty();
    return column->cloneEmpty();
}

ColumnPtr materializeColumn(const ColumnPtr & column)
{
    return recursiveRemoveLowCardinality(recursiveRemoveSparse(column->convertToFullColumnIfConst()));
}

ColumnRawPtrs materializeColumnsInplace(Block & block, const Names & names)
{
    ColumnRawPtrs ptrs;
    ptrs.reserve(names.size());

    for (const auto & column_name : names)
    {
        auto & column = block.getByName(column_name).column;
        column = materializeColumn(column);
        ptrs.push_back(column.get());
    }

    return ptrs;
}

ColumnPtr materializeColumn(const Block & block, const String & column_name)
{
    const auto & src_column = block.getByName(column_name).column;
    return materializeColumn(src_column);
}

Columns materializeColumns(const Block & block, const Names & names)
{
    Columns materialized;
    materialized.reserve(names.size());

    for (const auto & column_name : names)
    {
        materialized.emplace_back(materializeColumn(block, column_name));
    }

    return materialized;
}

ColumnRawPtrs getRawPointers(const Columns & columns)
{
    ColumnRawPtrs ptrs;
    ptrs.reserve(columns.size());

    for (const auto & column : columns)
        ptrs.push_back(column.get());

    return ptrs;
}

void restoreLowCardinalityInplace(Block & block, const Names & lowcard_keys)
{
    for (const auto & column_name : lowcard_keys)
    {
        if (!block.has(column_name))
            continue;
        if (auto & col = block.getByName(column_name); !col.type->lowCardinality())
            JoinCommon::changeLowCardinalityInplace(col);
    }

    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & col = block.getByPosition(i);
        if (col.type->lowCardinality() && col.column && !col.column->lowCardinality())
            col.column = changeLowCardinality(col.column, col.type->createColumn());
    }
}

ColumnRawPtrs extractKeysForJoin(const Block & block_keys, const Names & key_names)
{
    size_t keys_size = key_names.size();
    ColumnRawPtrs key_columns(keys_size);

    for (size_t i = 0; i < keys_size; ++i)
    {
        const String & column_name = key_names[i];
        key_columns[i] = block_keys.getByName(column_name).column.get();

        /// We will join only keys, where all components are not NULL.
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*key_columns[i]))
            key_columns[i] = &nullable->getNestedColumn();

        if (const auto * sparse = checkAndGetColumn<ColumnSparse>(&*key_columns[i]))
            key_columns[i] = &sparse->getValuesColumn();
    }

    return key_columns;
}

void checkTypesOfKeys(const Block & block_left, const Names & key_names_left,
                      const Block & block_right, const Names & key_names_right)
{
    size_t keys_size = key_names_left.size();

    for (size_t i = 0; i < keys_size; ++i)
    {
        DataTypePtr left_type = removeNullable(recursiveRemoveLowCardinality(block_left.getByName(key_names_left[i]).type));
        DataTypePtr right_type = removeNullable(recursiveRemoveLowCardinality(block_right.getByName(key_names_right[i]).type));

        if (!left_type->equals(*right_type))
        {
            throw DB::Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Type mismatch of columns to JOIN by: {} :: {} at left, {} :: {} at right",
                key_names_left[i], left_type->getName(),
                key_names_right[i], right_type->getName());
        }
    }
}

void checkTypesOfKeys(const Block & block_left, const Names & key_names_left, const String & condition_name_left,
                      const Block & block_right, const Names & key_names_right, const String & condition_name_right)
{
    checkTypesOfKeys(block_left, key_names_left, block_right, key_names_right);
    checkTypesOfMasks(block_left, condition_name_left, block_right, condition_name_right);
}

void checkTypesOfMasks(const Block & block_left, const String & condition_name_left,
                       const Block & block_right, const String & condition_name_right)
{
    auto check_cond_column_type = [](const Block & block, const String & col_name)
    {
        if (col_name.empty())
            return;

        DataTypePtr dtype = removeNullable(recursiveRemoveLowCardinality(block.getByName(col_name).type));

        if (!dtype->equals(DataTypeUInt8{}))
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                            "Expected logical expression in JOIN ON section, got unexpected column '{}' of type '{}'",
                            col_name, dtype->getName());
    };
    check_cond_column_type(block_left, condition_name_left);
    check_cond_column_type(block_right, condition_name_right);
}

void createMissedColumns(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & column = block.getByPosition(i);
        if (!column.column)
            column.column = column.type->createColumn();
    }
}

/// Append totals from right to left block, correct types if needed
void joinTotals(Block left_totals, Block right_totals, const TableJoin & table_join, Block & out_block)
{
    if (table_join.forceNullableLeft())
        JoinCommon::convertColumnsToNullable(left_totals);

    if (table_join.forceNullableRight())
        JoinCommon::convertColumnsToNullable(right_totals);

    for (auto & col : out_block)
    {
        if (const auto * left_col = left_totals.findByName(col.name))
            col = *left_col;
        else if (const auto * right_col = right_totals.findByName(col.name))
            col = *right_col;
        else
            col.column = col.type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();

        /// In case of using `arrayJoin` we can get more or less rows than one
        if (col.column->size() != 1)
            col.column = col.column->cloneResized(1);
    }
}

void addDefaultValues(IColumn & column, const DataTypePtr & type, size_t count)
{
    column.reserve(column.size() + count);
    for (size_t i = 0; i < count; ++i)
        type->insertDefaultInto(column);
}

bool typesEqualUpToNullability(DataTypePtr left_type, DataTypePtr right_type)
{
    DataTypePtr left_type_strict = removeNullable(removeLowCardinality(left_type));
    DataTypePtr right_type_strict = removeNullable(removeLowCardinality(right_type));
    return left_type_strict->equals(*right_type_strict);
}

JoinMask getColumnAsMask(const Block & block, const String & column_name)
{
    if (column_name.empty())
        return JoinMask(true, block.rows());

    const auto & src_col = block.getByName(column_name);

    DataTypePtr col_type = recursiveRemoveLowCardinality(src_col.type);
    if (isNothing(col_type))
        return JoinMask(false, block.rows());

    if (const auto * const_cond = checkAndGetColumn<ColumnConst>(&*src_col.column))
    {
        return JoinMask(const_cond->getBool(0), block.rows());
    }

    ColumnPtr join_condition_col = materializeColumn(src_col.column);
    if (const auto * nullable_col = typeid_cast<const ColumnNullable *>(join_condition_col.get()))
    {
        if (isNothing(assert_cast<const DataTypeNullable &>(*col_type).getNestedType()))
            return JoinMask(false, block.rows());

        /// Return nested column with NULL set to false
        const auto & nest_col = assert_cast<const ColumnUInt8 &>(nullable_col->getNestedColumn());
        const auto & null_map = nullable_col->getNullMapColumn();

        auto res = ColumnUInt8::create(nullable_col->size(), 0);
        for (size_t i = 0, sz = nullable_col->size(); i < sz; ++i)
            res->getData()[i] = !null_map.getData()[i] && nest_col.getData()[i];
        return JoinMask(std::move(res));
    }
    return JoinMask(std::move(join_condition_col));
}


void splitAdditionalColumns(const Names & key_names, const Block & sample_block, Block & block_keys, Block & block_others)
{
    block_others = materializeBlock(sample_block);

    for (const String & column_name : key_names)
    {
        /// Extract right keys with correct keys order. There could be the same key names.
        if (!block_keys.has(column_name))
        {
            auto & col = block_others.getByName(column_name);
            block_keys.insert(col);
            block_others.erase(column_name);
        }
    }
}

template <Fn<size_t(size_t)> Sharder>
static IColumn::Selector hashToSelector(const WeakHash32 & hash, Sharder sharder)
{
    const auto & hashes = hash.getData();
    size_t num_rows = hashes.size();

    IColumn::Selector selector(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        selector[i] = sharder(intHashCRC32(hashes[i]));
    return selector;
}

template <Fn<size_t(size_t)> Sharder>
static Blocks scatterBlockByHashImpl(const Strings & key_columns_names, const Block & block, size_t num_shards, Sharder sharder)
{
    size_t num_rows = block.rows();
    size_t num_cols = block.columns();

    /// Use non-standard initial value so as not to degrade hash map performance inside shard that uses the same CRC32 algorithm.
    WeakHash32 hash(num_rows);
    for (const auto & key_name : key_columns_names)
    {
        ColumnPtr key_col = materializeColumn(block, key_name);
        hash.update(key_col->getWeakHash32());
    }
    auto selector = hashToSelector(hash, sharder);

    Blocks result;
    result.reserve(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
    {
        result.emplace_back(block.cloneEmpty());
    }

    for (size_t i = 0; i < num_cols; ++i)
    {
        auto dispatched_columns = block.getByPosition(i).column->scatter(num_shards, selector);
        assert(result.size() == dispatched_columns.size());
        for (size_t block_index = 0; block_index < num_shards; ++block_index)
        {
            result[block_index].getByPosition(i).column = std::move(dispatched_columns[block_index]);
        }
    }
    return result;
}

static Blocks scatterBlockByHashPow2(const Strings & key_columns_names, const Block & block, size_t num_shards)
{
    size_t mask = num_shards - 1;
    return scatterBlockByHashImpl(key_columns_names, block, num_shards, [mask](size_t hash) { return hash & mask; });
}

static Blocks scatterBlockByHashGeneric(const Strings & key_columns_names, const Block & block, size_t num_shards)
{
    return scatterBlockByHashImpl(key_columns_names, block, num_shards, [num_shards](size_t hash) { return hash % num_shards; });
}

Blocks scatterBlockByHash(const Strings & key_columns_names, const Block & block, size_t num_shards)
{
    if (num_shards == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of shards must be positive");
    if (likely(isPowerOf2(num_shards)))
        return scatterBlockByHashPow2(key_columns_names, block, num_shards);
    return scatterBlockByHashGeneric(key_columns_names, block, num_shards);
}

template<typename T>
static Blocks scatterBlockByHashForList(const Strings & key_columns_names, const T & blocks, size_t num_shards)
{
    std::vector<Blocks> scattered_blocks(num_shards);
    for (const auto & block : blocks)
    {
        if (block.rows() == 0)
            continue;
        auto scattered = scatterBlockByHash(key_columns_names, block, num_shards);
        for (size_t i = 0; i < num_shards; ++i)
            scattered_blocks[i].emplace_back(std::move(scattered[i]));
    }

    Blocks result;
    result.reserve(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
    {
        result.emplace_back(concatenateBlocks(scattered_blocks[i]));
    }
    return result;
}

Blocks scatterBlockByHash(const Strings & key_columns_names, const Blocks & blocks, size_t num_shards)
{
    return scatterBlockByHashForList(key_columns_names, blocks, num_shards);
}

Blocks scatterBlockByHash(const Strings & key_columns_names, const BlocksList & blocks, size_t num_shards)
{
    return scatterBlockByHashForList(key_columns_names, blocks, num_shards);
}

bool hasNonJoinedBlocks(const TableJoin & table_join)
{
    return table_join.strictness() != JoinStrictness::Asof && table_join.strictness() != JoinStrictness::Semi
        && isRightOrFull(table_join.kind());
}

ColumnPtr filterWithBlanks(ColumnPtr src_column, const IColumn::Filter & filter, bool inverse_filter)
{
    ColumnPtr column = src_column->convertToFullColumnIfConst();
    MutableColumnPtr mut_column = column->cloneEmpty();
    mut_column->reserve(column->size());

    if (inverse_filter)
    {
        for (size_t row = 0; row < filter.size(); ++row)
        {
            if (filter[row])
                mut_column->insertDefault();
            else
                mut_column->insertFrom(*column, row);
        }
    }
    else
    {
        for (size_t row = 0; row < filter.size(); ++row)
        {
            if (filter[row])
                mut_column->insertFrom(*column, row);
            else
                mut_column->insertDefault();
        }
    }

    return mut_column;
}

}

NotJoinedBlocks::NotJoinedBlocks(std::unique_ptr<RightColumnsFiller> filler_,
                     const Block & result_sample_block_,
                     size_t left_columns_count,
                     const TableJoin & table_join)
    : filler(std::move(filler_))
    , saved_block_sample(filler->getEmptyBlock())
    , result_sample_block(materializeBlock(result_sample_block_))
{
    const auto & left_to_right_key_remap = table_join.leftToRightKeyRemap();
    for (size_t left_pos = 0; left_pos < left_columns_count; ++left_pos)
    {
        /// We need right 'x' for 'RIGHT JOIN ... USING(x)'
        auto left_name = result_sample_block.getByPosition(left_pos).name;
        const auto & right_key = left_to_right_key_remap.find(left_name);
        if (right_key != left_to_right_key_remap.end())
        {
            size_t right_key_pos = saved_block_sample.getPositionByName(right_key->second);
            setRightIndex(right_key_pos, left_pos);
        }
        else
            column_indices_left.emplace_back(left_pos);
    }

    /// `saved_block_sample` may contains non unique column names, get any of them
    /// (e.g. in case of `... JOIN (SELECT a, a, b FROM table) as t2`)
    for (const auto & [right_name, right_pos] : saved_block_sample.getNamesToIndexesMap())
    {
        String column_name(right_name);
        if (table_join.getStorageJoin())
        {
            /// StorageJoin operates with original non qualified column names, so apply renaming here
            column_name = table_join.renamedRightColumnName(column_name);
        }

        /// Start from left_columns_count to don't remap left keys twice. We need only qualified right keys here
        /// `result_sample_block` may contains non unique column names, need to set index for all of them
        for (size_t result_pos = left_columns_count; result_pos < result_sample_block.columns(); ++result_pos)
        {
            const auto & result_name = result_sample_block.getByPosition(result_pos).name;
            if (result_name == column_name)
                setRightIndex(right_pos, result_pos);
        }
    }

    if (column_indices_left.size() + column_indices_right.size() + same_result_keys.size() != result_sample_block.columns())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Error in columns mapping in JOIN: assertion failed {} + {} + {} != {}; "
            "left_columns_count = {}, result_sample_block.columns = [{}], saved_block_sample.columns = [{}]",
            column_indices_left.size(), column_indices_right.size(), same_result_keys.size(), result_sample_block.columns(),
            left_columns_count, result_sample_block.dumpNames(), saved_block_sample.dumpNames());
    }
}

void NotJoinedBlocks::setRightIndex(size_t right_pos, size_t result_position)
{
    if (!column_indices_right.contains(right_pos))
    {
        column_indices_right[right_pos] = result_position;
        extractColumnChanges(right_pos, result_position);
    }
    else
        same_result_keys[result_position] = column_indices_right[right_pos];
}

void NotJoinedBlocks::extractColumnChanges(size_t right_pos, size_t result_pos)
{
    auto src_props = getLowcardAndNullability(saved_block_sample.getByPosition(right_pos).column);
    auto dst_props = getLowcardAndNullability(result_sample_block.getByPosition(result_pos).column);

    if (src_props.is_nullable != dst_props.is_nullable)
        right_nullability_changes.push_back({result_pos, dst_props.is_nullable});

    if (src_props.is_lowcard != dst_props.is_lowcard)
        right_lowcard_changes.push_back({result_pos, dst_props.is_lowcard});
}

void NotJoinedBlocks::correctLowcardAndNullability(Block & block)
{
    /// First correct LowCardinality, then Nullability,
    /// because LowCardinality(Nullable(T)) is possible, but not Nullable(LowCardinality(T))
    for (auto & [pos, added] : right_lowcard_changes)
    {
        auto & col = block.getByPosition(pos);
        if (added)
        {
            if (!col.type->lowCardinality())
                col.type = std::make_shared<DataTypeLowCardinality>(col.type);
            col.column = changeLowCardinality(col.column, col.type->createColumn());
        }
        else
        {
            col.column = recursiveRemoveLowCardinality(col.column);
            col.type = recursiveRemoveLowCardinality(col.type);
        }
    }

    for (auto & [pos, added] : right_nullability_changes)
    {
        auto & col = block.getByPosition(pos);
        if (added)
            JoinCommon::convertColumnToNullable(col);
        else
            JoinCommon::removeColumnNullability(col);
    }
}

void NotJoinedBlocks::addLeftColumns(Block & block, size_t rows_added) const
{
    for (size_t pos : column_indices_left)
    {
        auto & col = block.getByPosition(pos);

        auto mut_col = col.column->cloneEmpty();
        JoinCommon::addDefaultValues(*mut_col, col.type, rows_added);
        col.column = std::move(mut_col);
    }
}

void NotJoinedBlocks::addRightColumns(Block & block, MutableColumns & columns_right) const
{
    for (const auto & pr : column_indices_right)
    {
        auto & right_column = columns_right[pr.first];
        auto & result_column = block.getByPosition(pr.second).column;
        result_column = std::move(right_column);
    }
}

void NotJoinedBlocks::copySameKeys(Block & block) const
{
    for (const auto & pr : same_result_keys)
    {
        auto & src_column = block.getByPosition(pr.second).column;
        auto & dst_column = block.getByPosition(pr.first).column;
        JoinCommon::changeColumnRepresentation(src_column, dst_column);
    }
}

Block NotJoinedBlocks::nextImpl()
{
    Block result_block = result_sample_block.cloneEmpty();
    {
        Block right_block = filler->getEmptyBlock();
        MutableColumns columns_right = right_block.cloneEmptyColumns();
        size_t rows_added = filler->fillColumns(columns_right);
        if (rows_added == 0)
            return {};

        addLeftColumns(result_block, rows_added);
        addRightColumns(result_block, columns_right);
    }
    copySameKeys(result_block);
    correctLowcardAndNullability(result_block);

#ifndef NDEBUG
    assertBlocksHaveEqualStructure(result_block, result_sample_block, "NotJoinedBlocks");
#endif
    return result_block;
}

}
