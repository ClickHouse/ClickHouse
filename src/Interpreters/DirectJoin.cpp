#include <Interpreters/DirectJoin.h>

#include <Columns/ColumnNullable.h>
#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

static Block originalRightBlock(const Block & block, const TableJoin & table_join)
{
    Block original_right_block;
    for (const auto & col : block)
        original_right_block.insert({col.column, col.type, table_join.getOriginalName(col.name)});
    return original_right_block;
}

/// Converts `columns` from `source_sample_block` structure to `result_sample_block`.
/// Can select subset of columns and change types.
static MutableColumns convertBlockStructure(
    const Block & source_sample_block, const Block & result_sample_block, MutableColumns && columns, const PaddedPODArray<UInt8> & null_map)
{
    MutableColumns result_columns;
    for (const auto & out_sample_col : result_sample_block)
    {
        /// Some columns from result_sample_block may not be in source_sample_block,
        /// e.g. if they will be calculated later based on joined columns
        if (!source_sample_block.has(out_sample_col.name))
            continue;

        auto i = source_sample_block.getPositionByName(out_sample_col.name);
        if (columns[i] == nullptr)
        {
            throw DB::Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Can't find column '{}'", out_sample_col.name);
        }

        ColumnWithTypeAndName col = source_sample_block.getByPosition(i);
        if (!col.type->equals(*out_sample_col.type))
        {
            col.column = std::move(columns[i]);
            result_columns.push_back(IColumn::mutate(castColumnAccurate(col, out_sample_col.type)));
        }
        else
        {
            result_columns.push_back(std::move(columns[i]));
        }
        columns[i] = nullptr;

        if (result_columns.back()->isNullable())
        {
            assert_cast<ColumnNullable *>(result_columns.back().get())->applyNegatedNullMap(null_map);
        }
    }
    return result_columns;
}

DirectKeyValueJoin::DirectKeyValueJoin(std::shared_ptr<TableJoin> table_join_,
                                       const Block & right_sample_block_,
                                       std::shared_ptr<IKeyValueEntity> storage_)
    : table_join(table_join_)
    , storage(storage_)
    , right_sample_block(right_sample_block_)
    , log(&Poco::Logger::get("DirectKeyValueJoin"))
{
    if (!table_join->oneDisjunct() ||
        table_join->getOnlyClause().key_names_left.size() != 1 ||
        table_join->getOnlyClause().key_names_right.size() != 1)
    {
        throw DB::Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Not supported by direct JOIN");
    }

    bool allowed_inner = isInner(table_join->kind()) && (table_join->strictness() == JoinStrictness::All ||
                                                         table_join->strictness() == JoinStrictness::Any ||
                                                         table_join->strictness() != JoinStrictness::RightAny);

    bool allowed_left = isLeft(table_join->kind()) && (table_join->strictness() == JoinStrictness::Any ||
                                                       table_join->strictness() == JoinStrictness::All ||
                                                       table_join->strictness() == JoinStrictness::Semi ||
                                                       table_join->strictness() == JoinStrictness::Anti);
    if (!allowed_inner && !allowed_left)
    {
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "Strictness {} and kind {} is not supported by direct JOIN",
            table_join->strictness(), table_join->kind());
    }

    LOG_TRACE(log, "Using direct join");
}

bool DirectKeyValueJoin::addJoinedBlock(const Block &, bool)
{
    throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Unreachable code reached");
}

void DirectKeyValueJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
    {
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_sample_block, onexpr.key_names_right);
    }
}

void DirectKeyValueJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> &)
{
    const String & key_name = table_join->getOnlyClause().key_names_left[0];
    const ColumnWithTypeAndName & key_col = block.getByName(key_name);
    if (!key_col.column)
        return;

    NullMap null_map;
    Chunk joined_chunk = storage->getByKeys({key_col}, null_map);

    /// Expected right block may differ from structure in storage, because of `join_use_nulls` or we just select not all joined attributes
    Block original_right_block = originalRightBlock(right_sample_block, *table_join);
    Block sample_storage_block = storage->getSampleBlock();
    MutableColumns result_columns = convertBlockStructure(sample_storage_block, original_right_block, joined_chunk.mutateColumns(), null_map);

    for (size_t i = 0; i < result_columns.size(); ++i)
    {
        ColumnWithTypeAndName col = right_sample_block.getByPosition(i);
        col.column = std::move(result_columns[i]);
        block.insert(std::move(col));
    }

    bool is_semi_join = table_join->strictness() == JoinStrictness::Semi;
    bool is_anti_join = table_join->strictness() == JoinStrictness::Anti;

    if (is_anti_join)
    {
        /// invert null_map
        for (auto & val : null_map)
            val = !val;
    }

    /// Filter non joined rows
    if (isInner(table_join->kind()) || (isLeft(table_join->kind()) && (is_semi_join || is_anti_join)))
    {
        MutableColumns dst_columns = block.mutateColumns();
        for (auto & col : dst_columns)
        {
            col = IColumn::mutate(col->filter(null_map, -1));
        }
        block.setColumns(std::move(dst_columns));
    }
}

}
