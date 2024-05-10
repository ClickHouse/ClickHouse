#pragma once
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/HashJoin/JoinFeatures.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/KnowRowsHolder.h>
#include <Interpreters//HashJoin/JoinUsedFlags.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/castColumn.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
/// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
template <typename HashMap, typename KeyGetter>
struct Inserter
{
    static ALWAYS_INLINE bool
    insertOne(const HashJoin & join, HashMap & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool);

        if (emplace_result.isInserted() || join.anyTakeLastRow())
        {
            new (&emplace_result.getMapped()) typename HashMap::mapped_type(stored_block, i);
            return true;
        }
        return false;
    }

    static ALWAYS_INLINE void insertAll(const HashJoin &, HashMap & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool);

        if (emplace_result.isInserted())
            new (&emplace_result.getMapped()) typename HashMap::mapped_type(stored_block, i);
        else
        {
            /// The first element of the list is stored in the value of the hash table, the rest in the pool.
            emplace_result.getMapped().insert({stored_block, i}, pool);
        }
    }

    static ALWAYS_INLINE void insertAsof(
        HashJoin & join, HashMap & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool, const IColumn & asof_column)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool);
        typename HashMap::mapped_type * time_series_map = &emplace_result.getMapped();

        TypeIndex asof_type = *join.getAsofType();
        if (emplace_result.isInserted())
            time_series_map = new (time_series_map) typename HashMap::mapped_type(createAsofRowRef(asof_type, join.getAsofInequality()));
        (*time_series_map)->insert(asof_column, stored_block, i);
    }
};

/// MapsTemplate is one of MapsOne, MapsAll and MapsAsof
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
class HashJoinMethods
{
public:
    static size_t insertFromBlockImpl(
        HashJoin & join,
        HashJoin::Type type,
        MapsTemplate & maps,
        size_t rows,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        Block * stored_block,
        ConstNullMapPtr null_map,
        UInt8ColumnDataPtr join_mask,
        Arena & pool,
        bool & is_inserted);

    using MapsTemplateVector = std::vector<const MapsTemplate *>;

    static Block joinBlockImpl(
        const HashJoin & join,
        Block & block,
        const Block & block_with_columns_to_add,
        const MapsTemplateVector & maps_,
        bool is_join_get = false);

private:
    template <typename KeyGetter, bool is_asof_join>
    static KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes);

    template <typename KeyGetter, typename HashMap>
    static size_t insertFromBlockImplTypeCase(
        HashJoin & join, HashMap & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, UInt8ColumnDataPtr join_mask, Arena & pool, bool & is_inserted);

    template <typename AddedColumns>
    static size_t switchJoinRightColumns(
        const std::vector<const MapsTemplate *> & mapv,
        AddedColumns & added_columns,
        HashJoin::Type type,
        JoinStuff::JoinUsedFlags & used_flags);

    template <typename KeyGetter, typename Map, typename AddedColumns>
    static size_t joinRightColumnsSwitchNullability(
        std::vector<KeyGetter> && key_getter_vector,
        const std::vector<const Map *> & mapv,
        AddedColumns & added_columns,
        JoinStuff::JoinUsedFlags & used_flags);

    template <typename KeyGetter, typename Map, bool need_filter, typename AddedColumns>
    static size_t joinRightColumnsSwitchMultipleDisjuncts(
        std::vector<KeyGetter> && key_getter_vector,
        const std::vector<const Map *> & mapv,
        AddedColumns & added_columns,
        JoinStuff::JoinUsedFlags & used_flags);

    /// Joins right table columns which indexes are present in right_indexes using specified map.
    /// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
    template <typename KeyGetter, typename Map, bool need_filter, bool flag_per_row, typename AddedColumns>
    static size_t joinRightColumns(
        std::vector<KeyGetter> && key_getter_vector,
        const std::vector<const Map *> & mapv,
        AddedColumns & added_columns,
        JoinStuff::JoinUsedFlags & used_flags)
    {
        constexpr JoinFeatures<KIND, STRICTNESS> join_features;

        size_t rows = added_columns.rows_to_add;
        if constexpr (need_filter)
            added_columns.filter = IColumn::Filter(rows, 0);
        if constexpr (!flag_per_row && (STRICTNESS == JoinStrictness::All || (STRICTNESS == JoinStrictness::Semi && KIND == JoinKind::Right)))
            added_columns.output_by_row_list = true;

        Arena pool;

        if constexpr (join_features.need_replication)
            added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

        IColumn::Offset current_offset = 0;
        size_t max_joined_block_rows = added_columns.max_joined_block_rows;
        size_t i = 0;
        for (; i < rows; ++i)
        {
            if constexpr (join_features.need_replication)
            {
                if (unlikely(current_offset >= max_joined_block_rows))
                {
                    added_columns.offsets_to_replicate->resize_assume_reserved(i);
                    added_columns.filter.resize_assume_reserved(i);
                    break;
                }
            }

            bool right_row_found = false;

            KnownRowsHolder<flag_per_row> known_rows;
            for (size_t onexpr_idx = 0; onexpr_idx < added_columns.join_on_keys.size(); ++onexpr_idx)
            {
                const auto & join_keys = added_columns.join_on_keys[onexpr_idx];
                if (join_keys.null_map && (*join_keys.null_map)[i])
                        continue;

                bool row_acceptable = !join_keys.isRowFiltered(i);
                using FindResult = typename KeyGetter::FindResult;
                auto find_result = row_acceptable ? key_getter_vector[onexpr_idx].findKey(*(mapv[onexpr_idx]), i, pool) : FindResult();

                if (find_result.isFound())
                {
                    right_row_found = true;
                    auto & mapped = find_result.getMapped();
                    if constexpr (join_features.is_asof_join)
                    {
                        const IColumn & left_asof_key = added_columns.leftAsofKey();

                        auto row_ref = mapped->findAsof(left_asof_key, i);
                        if (row_ref && row_ref->block)
                        {
                            setUsed<need_filter>(added_columns.filter, i);
                            if constexpr (flag_per_row)
                                used_flags.template setUsed<join_features.need_flags, flag_per_row>(row_ref->block, row_ref->row_num, 0);
                            else
                                used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);

                            added_columns.appendFromBlock(row_ref, join_features.add_missing);
                        }
                        else
                            addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
                    }
                    else if constexpr (join_features.is_all_join)
                    {
                        setUsed<need_filter>(added_columns.filter, i);
                        used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
                        auto used_flags_opt = join_features.need_flags ? &used_flags : nullptr;
                        addFoundRowAll<Map, join_features.add_missing>(mapped, added_columns, current_offset, known_rows, used_flags_opt);
                    }
                    else if constexpr ((join_features.is_any_join || join_features.is_semi_join) && join_features.right)
                    {
                        /// Use first appeared left key + it needs left columns replication
                        bool used_once = used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result);
                        if (used_once)
                        {
                            auto used_flags_opt = join_features.need_flags ? &used_flags : nullptr;
                            setUsed<need_filter>(added_columns.filter, i);
                            addFoundRowAll<Map, join_features.add_missing>(
                                mapped, added_columns, current_offset, known_rows, used_flags_opt);
                        }
                    }
                    else if constexpr (join_features.is_any_join && KIND == JoinKind::Inner)
                    {
                        bool used_once = used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result);

                        /// Use first appeared left key only
                        if (used_once)
                        {
                            setUsed<need_filter>(added_columns.filter, i);
                            added_columns.appendFromBlock(&mapped, join_features.add_missing);
                        }

                        break;
                    }
                    else if constexpr (join_features.is_any_join && join_features.full)
                    {
                        /// TODO
                    }
                    else if constexpr (join_features.is_anti_join)
                    {
                        if constexpr (join_features.right && join_features.need_flags)
                            used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
                    }
                    else /// ANY LEFT, SEMI LEFT, old ANY (RightAny)
                    {
                        setUsed<need_filter>(added_columns.filter, i);
                        used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
                        added_columns.appendFromBlock(&mapped, join_features.add_missing);

                        if (join_features.is_any_or_semi_join)
                        {
                            break;
                        }
                    }
                }
            }

            if (!right_row_found)
            {
                if constexpr (join_features.is_anti_join && join_features.left)
                    setUsed<need_filter>(added_columns.filter, i);
                addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
            }

            if constexpr (join_features.need_replication)
            {
                (*added_columns.offsets_to_replicate)[i] = current_offset;
            }
        }

        added_columns.applyLazyDefaults();
        return i;
    }

    template <bool need_filter>
    static void setUsed(IColumn::Filter & filter [[maybe_unused]], size_t pos [[maybe_unused]]);

    template <typename AddedColumns>
    static ColumnPtr buildAdditionalFilter(
        size_t left_start_row,
        const std::vector<const RowRef *> & selected_rows,
        const std::vector<size_t> & row_replicate_offset,
        AddedColumns & added_columns)
    {
        ColumnPtr result_column;
        do
        {
            if (selected_rows.empty())
            {
                result_column = ColumnUInt8::create();
                break;
            }
            const Block & sample_right_block = *((*selected_rows.begin())->block);
            if (!sample_right_block || !added_columns.additional_filter_expression)
            {
                auto filter = ColumnUInt8::create();
                filter->insertMany(1, selected_rows.size());
                result_column = std::move(filter);
                break;
            }

            auto required_cols = added_columns.additional_filter_expression->getRequiredColumnsWithTypes();
            if (required_cols.empty())
            {
                Block block;
                added_columns.additional_filter_expression->execute(block);
                result_column = block.getByPosition(0).column->cloneResized(selected_rows.size());
                break;
            }
            NameSet required_column_names;
            for (auto & col : required_cols)
                required_column_names.insert(col.name);

            Block executed_block;
            size_t right_col_pos = 0;
            for (const auto & col : sample_right_block.getColumnsWithTypeAndName())
            {
                if (required_column_names.contains(col.name))
                {
                    auto new_col = col.column->cloneEmpty();
                    for (const auto & selected_row : selected_rows)
                    {
                        const auto & src_col = selected_row->block->getByPosition(right_col_pos);
                        new_col->insertFrom(*src_col.column, selected_row->row_num);
                    }
                    executed_block.insert({std::move(new_col), col.type, col.name});
                }
                right_col_pos += 1;
            }
            if (!executed_block)
            {
                result_column = ColumnUInt8::create();
                break;
            }

            for (const auto & col_name : required_column_names)
            {
                const auto * src_col = added_columns.left_block.findByName(col_name);
                if (!src_col)
                    continue;
                auto new_col = src_col->column->cloneEmpty();
                size_t prev_left_offset = 0;
                for (size_t i = 1; i < row_replicate_offset.size(); ++i)
                {
                    const size_t & left_offset = row_replicate_offset[i];
                    size_t rows = left_offset - prev_left_offset;
                    if (rows)
                        new_col->insertManyFrom(*src_col->column, left_start_row + i - 1, rows);
                    prev_left_offset = left_offset;
                }
                executed_block.insert({std::move(new_col), src_col->type, col_name});
            }
            if (!executed_block)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "required columns: [{}], but not found any in left/right table. right table: {}, left table: {}",
                    required_cols.toString(),
                    sample_right_block.dumpNames(),
                    added_columns.left_block.dumpNames());
            }

            for (const auto & col : executed_block.getColumnsWithTypeAndName())
                if (!col.column || !col.type)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal nullptr column in input block: {}", executed_block.dumpStructure());

            added_columns.additional_filter_expression->execute(executed_block);
            result_column = executed_block.getByPosition(0).column->convertToFullColumnIfConst();
            executed_block.clear();
        } while (false);

        result_column = result_column->convertToFullIfNeeded();
        if (result_column->isNullable())
        {
            /// Convert Nullable(UInt8) to UInt8 ensuring that nulls are zeros
            /// Trying to avoid copying data, since we are the only owner of the column.
            ColumnPtr mask_column = assert_cast<const ColumnNullable &>(*result_column).getNullMapColumnPtr();

            MutableColumnPtr mutable_column;
            {
                ColumnPtr nested_column = assert_cast<const ColumnNullable &>(*result_column).getNestedColumnPtr();
                result_column.reset();
                mutable_column = IColumn::mutate(std::move(nested_column));
            }

            auto & column_data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
            const auto & mask_column_data = assert_cast<const ColumnUInt8 &>(*mask_column).getData();
            for (size_t i = 0; i < column_data.size(); ++i)
            {
                if (mask_column_data[i])
                    column_data[i] = 0;
            }
            return mutable_column;
        }
        return result_column;
    }

    /// First to collect all matched rows refs by join keys, then filter out rows which are not true in additional filter expression.
    template <typename KeyGetter, typename Map, typename AddedColumns>
    static size_t joinRightColumnsWithAddtitionalFilter(
        std::vector<KeyGetter> && key_getter_vector,
        const std::vector<const Map *> & mapv,
        AddedColumns & added_columns,
        JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]],
        bool need_filter [[maybe_unused]],
        bool need_flags [[maybe_unused]],
        bool add_missing [[maybe_unused]],
        bool flag_per_row [[maybe_unused]])
    {
        size_t left_block_rows = added_columns.rows_to_add;
        if (need_filter)
            added_columns.filter = IColumn::Filter(left_block_rows, 0);

        std::unique_ptr<Arena> pool;

        if constexpr (need_replication)
            added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(left_block_rows);

        std::vector<size_t> row_replicate_offset;
        row_replicate_offset.reserve(left_block_rows);

        using FindResult = typename KeyGetter::FindResult;
        size_t max_joined_block_rows = added_columns.max_joined_block_rows;
        size_t left_row_iter = 0;
        PreSelectedRows selected_rows;
        selected_rows.reserve(left_block_rows);
        std::vector<FindResult> find_results;
        find_results.reserve(left_block_rows);
        bool exceeded_max_block_rows = false;
        IColumn::Offset total_added_rows = 0;
        IColumn::Offset current_added_rows = 0;

        auto collect_keys_matched_rows_refs = [&]()
        {
            pool = std::make_unique<Arena>();
            find_results.clear();
            row_replicate_offset.clear();
            row_replicate_offset.push_back(0);
            current_added_rows = 0;
            selected_rows.clear();
            for (; left_row_iter < left_block_rows; ++left_row_iter)
            {
                if constexpr (need_replication)
                {
                    if (unlikely(total_added_rows + current_added_rows >= max_joined_block_rows))
                    {
                        break;
                    }
                }
                KnownRowsHolder<true> all_flag_known_rows;
                KnownRowsHolder<false> single_flag_know_rows;
                for (size_t join_clause_idx = 0; join_clause_idx < added_columns.join_on_keys.size(); ++join_clause_idx)
                {
                    const auto & join_keys = added_columns.join_on_keys[join_clause_idx];
                    if (join_keys.null_map && (*join_keys.null_map)[left_row_iter])
                        continue;

                    bool row_acceptable = !join_keys.isRowFiltered(left_row_iter);
                    auto find_result = row_acceptable
                        ? key_getter_vector[join_clause_idx].findKey(*(mapv[join_clause_idx]), left_row_iter, *pool)
                        : FindResult();

                    if (find_result.isFound())
                    {
                        auto & mapped = find_result.getMapped();
                        find_results.push_back(find_result);
                        if (flag_per_row)
                            addFoundRowAll<Map, false, true>(mapped, selected_rows, current_added_rows, all_flag_known_rows, nullptr);
                        else
                            addFoundRowAll<Map, false, false>(mapped, selected_rows, current_added_rows, single_flag_know_rows, nullptr);
                    }
                }
                row_replicate_offset.push_back(current_added_rows);
            }
        };

        auto copy_final_matched_rows = [&](size_t left_start_row, ColumnPtr filter_col)
        {
            const PaddedPODArray<UInt8> & filter_flags = assert_cast<const ColumnUInt8 &>(*filter_col).getData();

            size_t prev_replicated_row = 0;
            auto selected_right_row_it = selected_rows.begin();
            size_t find_result_index = 0;
            for (size_t i = 1, n = row_replicate_offset.size(); i < n; ++i)
            {
                bool any_matched = false;
                /// For all right join, flag_per_row is true, we need mark used flags for each row.
                if (flag_per_row)
                {
                    for (size_t replicated_row = prev_replicated_row; replicated_row < row_replicate_offset[i]; ++replicated_row)
                    {
                        if (filter_flags[replicated_row])
                        {
                            any_matched = true;
                            added_columns.appendFromBlock(*selected_right_row_it, add_missing);
                            total_added_rows += 1;
                            if (need_flags)
                                used_flags.template setUsed<true, true>((*selected_right_row_it)->block, (*selected_right_row_it)->row_num, 0);
                        }
                        ++selected_right_row_it;
                    }
                }
                else
                {
                    for (size_t replicated_row = prev_replicated_row; replicated_row < row_replicate_offset[i]; ++replicated_row)
                    {
                        if (filter_flags[replicated_row])
                        {
                            any_matched = true;
                            added_columns.appendFromBlock(*selected_right_row_it, add_missing);
                            total_added_rows += 1;
                        }
                        ++selected_right_row_it;
                    }
                }
                if (!any_matched)
                {
                    if (add_missing)
                        addNotFoundRow<true, need_replication>(added_columns, total_added_rows);
                    else
                        addNotFoundRow<false, need_replication>(added_columns, total_added_rows);
                }
                else
                {
                    if (!flag_per_row && need_flags)
                        used_flags.template setUsed<true, false>(find_results[find_result_index]);
                    if (need_filter)
                        setUsed<true>(added_columns.filter, left_start_row + i - 1);
                    if (add_missing)
                        added_columns.applyLazyDefaults();
                }
                find_result_index += (prev_replicated_row != row_replicate_offset[i]);

                if constexpr (need_replication)
                {
                    (*added_columns.offsets_to_replicate)[left_start_row + i - 1] = total_added_rows;
                }
                prev_replicated_row = row_replicate_offset[i];
            }
        };

        while (left_row_iter < left_block_rows && !exceeded_max_block_rows)
        {
            auto left_start_row = left_row_iter;
            collect_keys_matched_rows_refs();
            if (selected_rows.size() != current_added_rows || row_replicate_offset.size() != left_row_iter - left_start_row + 1)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Sizes are mismatched. selected_rows.size:{}, current_added_rows:{}, row_replicate_offset.size:{}, left_row_iter: {}, "
                    "left_start_row: {}",
                    selected_rows.size(),
                    current_added_rows,
                    row_replicate_offset.size(),
                    left_row_iter,
                    left_start_row);
            }
            auto filter_col = buildAdditionalFilter(left_start_row, selected_rows, row_replicate_offset, added_columns);
            copy_final_matched_rows(left_start_row, filter_col);

            if constexpr (need_replication)
            {
                // Add a check for current_added_rows to avoid run the filter expression on too small size batch.
                if (total_added_rows >= max_joined_block_rows || current_added_rows < 1024)
                    exceeded_max_block_rows = true;
            }
        }

        if constexpr (need_replication)
        {
            added_columns.offsets_to_replicate->resize_assume_reserved(left_row_iter);
            added_columns.filter.resize_assume_reserved(left_row_iter);
        }
        added_columns.applyLazyDefaults();
        return left_row_iter;
    }

    /// Cut first num_rows rows from block in place and returns block with remaining rows
    static Block sliceBlock(Block & block, size_t num_rows);

    /** Since we do not store right key columns,
      * this function is used to copy left key columns to right key columns.
      * If the user requests some right columns, we just copy left key columns to right, since they are equal.
      * Example: SELECT t1.key, t2.key FROM t1 FULL JOIN t2 ON t1.key = t2.key;
      * In that case for matched rows in t2.key we will use values from t1.key.
      * However, in some cases we might need to adjust the type of column, e.g. t1.key :: LowCardinality(String) and t2.key :: String
      * Also, the nullability of the column might be different.
      * Returns the right column after with necessary adjustments.
      */
    static ColumnWithTypeAndName copyLeftKeyColumnToRight(
        const DataTypePtr & right_key_type,
        const String & renamed_right_column,
        const ColumnWithTypeAndName & left_column,
        const IColumn::Filter * null_map_filter = nullptr);

    static void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable);

    static void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable, const IColumn::Filter & negative_null_map);
};

/// Instantiate template class ahead in different .cpp files to avoid `too large translation unit`.
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::RightAny, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Any, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Any, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::All, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Semi, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Semi, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Anti, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Anti, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Asof, HashJoin::MapsAsof>;

extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::RightAny, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::Any, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::All, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::Semi, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::Anti, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::Asof, HashJoin::MapsAsof>;

extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::RightAny, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Any, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Any, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::All, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Semi, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Anti, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Asof, HashJoin::MapsAsof>;

extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::RightAny, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::Any, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::All, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::Semi, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::Anti, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::Asof, HashJoin::MapsAsof>;
}

