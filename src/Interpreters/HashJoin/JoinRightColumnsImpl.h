#pragma once

#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/AmacLookup.h>
#include <Interpreters/HashJoin/FindKeys.h>
#include <Interpreters/HashJoin/JoinFeatures.h>
#include <Interpreters/HashJoin/JoinRightColumns.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/KnownRowsHolder.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>

#include <type_traits>

/// The match step of the hash join probe, and `selectFunctions`. Only the files that instantiate the methods of
/// a join kind include it.
namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// With `claim_flags` set, a row is appended only if this call wins its used flag, so it is emitted
/// for exactly one left row; pass nullptr when a row may be emitted more than once (ALL joins).
/// Returns whether anything was appended.
template <bool flag_per_row, typename AddedColumns>
bool addFoundRowAll(
    const RowRefList & mapped,
    AddedColumns & added,
    IColumn::Offset & current_offset,
    KnownRowsHolder<flag_per_row> & known_rows [[maybe_unused]],
    JoinStuff::JoinUsedFlags * claim_flags [[maybe_unused]],
    bool is_last_disjunct [[maybe_unused]])
{
    if constexpr (flag_per_row)
    {
        std::vector<UInt64> new_known_rows;
        bool any_row_added = false;

        for (const UInt64 ref_word : refsOf(mapped.word))
        {
            if (known_rows.isKnown(ref_word))
                continue;

            if (claim_flags
                && !claim_flags->JoinStuff::JoinUsedFlags::setUsedOnce<true, flag_per_row>(
                    refWordBlockNo(ref_word), refWordRowNo(ref_word), 0))
                continue;

            added.appendFromBlock(ref_word);
            ++current_offset;
            any_row_added = true;
            if (!is_last_disjunct)
                new_known_rows.push_back(ref_word);
        }

        if (!is_last_disjunct)
            known_rows.add(std::cbegin(new_known_rows), std::cend(new_known_rows));

        return any_row_added;
    }
    else
    {
        /// A single ref lives inline in the cell word, and the iterator decodes it without touching the arena node.
        for (const UInt64 ref_word : refsOf(mapped.word))
        {
            added.appendFromBlock(ref_word);
            ++current_offset;
        }
        return true;
    }
}

template <bool add_missing, bool need_offset, typename AddedColumns>
void addNotFoundRow(AddedColumns & added [[maybe_unused]], IColumn::Offset & current_offset [[maybe_unused]])
{
    if constexpr (add_missing)
    {
        added.appendDefaultRow();
        if constexpr (need_offset)
            ++current_offset;
    }
}

template <bool need_filter>
void setUsed(IColumn::Filter & filter [[maybe_unused]], size_t pos [[maybe_unused]], IColumn::Offsets & matched_rows [[maybe_unused]])
{
    if constexpr (need_filter)
    {
        filter[pos] = 1;
        matched_rows.push_back(pos);
    }
}

template <typename AddedColumns>
static ColumnPtr buildAdditionalFilter(
    size_t first_row,
    const PODArray<UInt64> & selected_rows,
    const IColumn::Offsets & row_replicate_offset,
    const AddedColumns & added_columns)
{
    ColumnPtr result_column;
    do
    {
        if (selected_rows.empty())
        {
            result_column = ColumnUInt8::create();
            break;
        }

        if (!added_columns.additional_filter_expression)
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

        ColumnsWithTypeAndName required_columns;
        required_columns.reserve(required_cols.size());
        auto rhs_pos_it = added_columns.additional_filter_required_rhs_pos.begin();
        auto req_cols_it = required_cols.begin();
        for (size_t pos = 0; pos < required_cols.size(); ++pos, ++req_cols_it)
        {
            if (rhs_pos_it != added_columns.additional_filter_required_rhs_pos.end() && pos == rhs_pos_it->first)
            {
                const auto & req_col = *req_cols_it;
                required_columns.emplace_back(nullptr, req_col.type, req_col.name);

                auto col = req_col.type->createColumn();
                for (const UInt64 selected_row : selected_rows)
                {
                    const auto * block = added_columns.lazy_output.stored_columns[refWordBlockNo(selected_row)];
                    const auto [src_col, row_pos] = getBlockColumnAndRow(block, refWordRowNo(selected_row), rhs_pos_it->second);
                    col->insertFrom(*src_col, row_pos);
                }
                required_columns[pos].column = std::move(col);
                ++rhs_pos_it;
            }
            else
            {
                const auto & col_name = req_cols_it->name;
                const auto * src_col = added_columns.left_block.findByName(col_name);
                if (!src_col)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "required columns: [{}], but not found any in left table. left table: {}, required column: {}",
                        required_cols.toString(),
                        added_columns.left_block.dumpNames(),
                        col_name);

                auto new_col = src_col->column->cloneEmpty();
                for (size_t i = 0; i < row_replicate_offset.size(); ++i)
                {
                    size_t rows = row_replicate_offset[i] - row_replicate_offset[i - 1];
                    if (rows)
                    {
                        new_col->insertManyFrom(*src_col->column, first_row + i, rows);
                    }
                }
                required_columns.push_back({std::move(new_col), src_col->type, col_name});
            }
        }

        Block executed_block(std::move(required_columns));

        for (const auto & col : executed_block.getColumnsWithTypeAndName())
            if (!col.column || !col.type)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal nullptr column in input block: {}", executed_block.dumpStructure());

        added_columns.additional_filter_expression->execute(executed_block);
        result_column = executed_block.getByPosition(0).column->convertToFullColumnIfConst();
        executed_block.clear();
    } while (false);

    result_column = result_column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
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

namespace JoinProbe
{

/// What a match does to the used flag of its key in a join with one disjunct.
enum class UsedFlagAction : uint8_t
{
    None,
    /// RIGHT and FULL ALL, RIGHT and FULL old ANY, RIGHT ANTI: the scan after the probe emits the keys nobody used.
    SetUsed,
    /// INNER ANY, RIGHT ANY, RIGHT SEMI: the first left row to set the flag takes the key. For the other rows it is no match.
    SetUsedOnce,
};

/// What a join kind does with a match and with a row that has none, derived from `JoinFeatures`.
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
struct MatchRules
{
    using Features = JoinFeatures<KIND, STRICTNESS, MapsTemplate>;
    using Maps = MapsTemplate;
    static constexpr JoinKind kind = KIND;
    static constexpr JoinStrictness strictness = STRICTNESS;

    enum class OnMatch : uint8_t
    {
        /// ALL, and RIGHT ANY or RIGHT SEMI once the row took the key: the left row is replicated for every row of the key.
        AllRowsOfKey,
        /// LEFT and INNER ANY, SEMI, old ANY.
        FirstRowOfKey,
        /// The row that `findAsof` picks, or no match if there is none.
        AsofRow,
        /// SEMI on a set map: the left row is kept, with no right row.
        LeftRowOnly,
        /// ANTI, and FULL ANY, for which the probe has never had an implementation.
        NoOutput,
    };

    static constexpr OnMatch on_match = []
    {
        if constexpr (SetJoinMaps<Maps>)
            return Features::is_semi_join ? OnMatch::LeftRowOnly : OnMatch::NoOutput;
        else if constexpr (Features::is_asof_join)
            return OnMatch::AsofRow;
        else if constexpr (Features::is_all_join || ((Features::is_any_join || Features::is_semi_join) && Features::right))
            return OnMatch::AllRowsOfKey;
        else if constexpr ((Features::is_any_join && Features::full) || Features::is_anti_join)
            return OnMatch::NoOutput;
        else
            return OnMatch::FirstRowOfKey;
    }();

    static constexpr UsedFlagAction used_flag_action = []
    {
        if constexpr (!Features::need_flags || SetJoinMaps<Maps> || Features::is_asof_join || (Features::is_any_join && Features::full))
            return UsedFlagAction::None;
        else if constexpr (
            (Features::is_any_join && Features::inner) || ((Features::is_any_join || Features::is_semi_join) && Features::right))
            return UsedFlagAction::SetUsedOnce;
        else if constexpr (Features::is_anti_join)
            return Features::right ? UsedFlagAction::SetUsed : UsedFlagAction::None;
        else
            return UsedFlagAction::SetUsed;
    }();

    static constexpr bool adds_row_ref
        = on_match == OnMatch::AllRowsOfKey || on_match == OnMatch::FirstRowOfKey || on_match == OnMatch::AsofRow;
    static constexpr bool sets_filter_on_match = adds_row_ref || on_match == OnMatch::LeftRowOnly;
    static constexpr bool keeps_rows_without_match = Features::is_anti_join && Features::left;
    static constexpr bool stops_at_first_match
        = Features::is_any_or_semi_join && !(Features::is_any_join && (Features::right || Features::full));

    /// The joins for which one fused loop is faster than two steps.
    static constexpr bool fused_with_one_disjunct = SetJoinMaps<Maps> || Features::is_anti_join
        || (Features::is_any_join && Features::inner) || ((Features::right || Features::full) && !Features::is_all_join);
};

/// Adds the output of a join with one disjunct, one row at a time, from what the key search found for the row. For each
/// row it applies the used flag action on `key_offset` (a lost claim makes the row a miss) and picks the ASOF row. Then it
/// writes the filter, the replication offset and the row ref. The rows must come in row order. The row refs are kept in
/// order at the front of `JoinRightColumns::match_words`.
/// With `branch_free`, `add` serves the match step after the key search and writes every row. A missing key must then have
/// the offset `HashJoinTable::missingKeyOffset`, whose flag nothing reads. Without `branch_free`, `addFound` and `addMiss`
/// serve a fused loop and write only the rows with output.
template <typename Rules, bool need_filter, bool branch_free>
class MatchAdder
{
public:
    ALWAYS_INLINE explicit MatchAdder(JoinRightColumns & join_)
        : join(join_)
        , first_row(join_.batch_first_row)
        , kept_words(join_.match_words)
        , record_row_refs(join_.added_columns.record_row_refs)
    {
        AddedColumns & added_columns = join.added_columns;
        const size_t rows = join.batch_rows;
        if constexpr (need_filter)
        {
            if constexpr (branch_free)
                added_columns.filter.resize(rows);
            else
                added_columns.filter.resize_fill(rows, 0);
            added_columns.matched_rows.resize(rows);
            filter = added_columns.filter.data();
            matched = added_columns.matched_rows.data();
        }
        if constexpr (Features::need_replication)
        {
            added_columns.offsets_to_replicate.resize(rows);
            offsets = added_columns.offsets_to_replicate.data();
        }
        if constexpr (Rules::used_flag_action != UsedFlagAction::None)
            offset_flags = join.used_flags.offsetFlags();
        if constexpr (Rules::on_match == OnMatch::AsofRow)
            asof_key = &added_columns.leftAsofKey();
    }

    /// Nothing here branches on whether the row matched: when about half of the rows match, such a branch is unpredictable.
    ALWAYS_INLINE void add(size_t i, MatchWord word, size_t key_offset [[maybe_unused]])
    {
        static_assert(branch_free);
        if constexpr (Rules::used_flag_action == UsedFlagAction::SetUsedOnce)
            word = JoinStuff::JoinUsedFlags::setUsedOnceAt(offset_flags, key_offset) ? word : 0;
        else if constexpr (Rules::used_flag_action == UsedFlagAction::SetUsed)
            JoinStuff::JoinUsedFlags::setUsedAt(offset_flags, key_offset);
        addEveryRow(i, asofRow(i, word));
    }

    /// For a fused loop, which branches on the match anyway: a row whose key was found. `word_of_key` gives its match word.
    /// The word is read only when the row needs it, after the claim of an ANY join.
    template <typename WordOfKey>
    ALWAYS_INLINE void addFound(size_t i, size_t key_offset [[maybe_unused]], WordOfKey && word_of_key)
    {
        static_assert(!branch_free);
        if constexpr (Rules::used_flag_action == UsedFlagAction::SetUsedOnce)
        {
            if (!JoinStuff::JoinUsedFlags::setUsedOnceAt(offset_flags, key_offset))
                return addMiss(i);
        }
        else if constexpr (Rules::used_flag_action == UsedFlagAction::SetUsed)
            JoinStuff::JoinUsedFlags::setUsedAt(offset_flags, key_offset);

        MatchWord word = 1;
        if constexpr (Rules::adds_row_ref)
            word = asofRow(i, word_of_key());
        if (Rules::on_match == OnMatch::AsofRow && !word)
            return addMiss(i);
        addMatch(i, word);
    }

    /// For a fused loop: a row whose key was not found.
    ALWAYS_INLINE void addMiss(size_t i [[maybe_unused]])
    {
        static_assert(!branch_free);
        if constexpr (need_filter && Rules::keeps_rows_without_match)
        {
            filter[i] = 1;
            matched[matched_count++] = i;
        }
        if constexpr (Features::add_missing)
        {
            if (record_row_refs)
            {
                kept_words[kept++] = 0;
                ++row_count;
            }
            if constexpr (Features::need_replication)
                ++offset;
        }
        if constexpr (Features::need_replication)
            offsets[i] = offset;
    }

    ALWAYS_INLINE size_t finish()
    {
        AddedColumns & added_columns = join.added_columns;
        if constexpr (need_filter)
            added_columns.matched_rows.resize(matched_count);

        auto & lazy_output = added_columns.lazy_output;
        if (record_row_refs)
        {
            if constexpr (branch_free && keeps_words_in_place)
                kept = join.batch_rows;
            lazy_output.row_refs.resize_assume_reserved(join.row_refs_base + kept);
            lazy_output.row_count += row_count;
            lazy_output.hash_table_matches += matches;
        }
        else
            lazy_output.row_refs.resize_assume_reserved(join.row_refs_base);

        if constexpr (Features::emits_whole_key_per_word)
            lazy_output.output_by_row_list = true;
        return join.batch_rows;
    }

private:
    using Features = typename Rules::Features;
    using OnMatch = typename Rules::OnMatch;
    static constexpr bool maps_all = std::is_same_v<typename Rules::Maps, HashJoin::MapsAll>;

    /// A join that adds a default row for a miss keeps one row ref per row. Where that ref is the match word itself, the
    /// words already are the row refs.
    static constexpr bool keeps_words_in_place = Features::add_missing && Rules::used_flag_action != UsedFlagAction::SetUsedOnce
        && (Rules::on_match == OnMatch::AllRowsOfKey || (Rules::on_match == OnMatch::FirstRowOfKey && !maps_all));

    ALWAYS_INLINE MatchWord asofRow(size_t i [[maybe_unused]], MatchWord word)
    {
        if constexpr (Rules::on_match == OnMatch::AsofRow)
        {
            if (word)
            {
                const RowRef * row = reinterpret_cast<SortedLookupVectorBase *>(word)->findAsof(*asof_key, first_row + i);
                word = row ? row->encode() : 0;
            }
        }
        return word;
    }

    ALWAYS_INLINE void addEveryRow(size_t i, MatchWord word)
    {
        const size_t hit = word != 0;
        size_t hit_rows = Rules::adds_row_ref ? hit : 0;
        MatchWord kept_word = word;
        if constexpr (Rules::on_match == OnMatch::AllRowsOfKey)
        {
            /// This is `refWordRows` with a select between an inline word and a list word. On unique keys, a branch there would
            /// be a branch on whether the row matched. A saturated count takes the slow call, and so does an inline word with
            /// the same bits set.
            const size_t count = (word >> RowRefList::COUNT_SHIFT) & RowRefList::COUNT_SAT;
            hit_rows = refWordIsInline(word) ? 1 : count;
            if (count == RowRefList::COUNT_SAT) [[unlikely]]
                hit_rows = refWordRows(word);
        }
        else if constexpr (Rules::on_match == OnMatch::FirstRowOfKey && maps_all)
        {
            /// Only a list word, which is neither 0 nor inline, needs the load. On unique keys this one test never passes.
            if (word - 1 < RowRef::ENCODED_INLINE_FLAG - 1)
                kept_word = RowRefList::fromWord(word).firstWord();
        }

        constexpr bool default_row = Features::add_missing;
        const size_t out_rows = default_row ? hit_rows + 1 - hit : hit_rows;
        if constexpr (need_filter)
        {
            size_t pass = 0;
            if constexpr (Rules::sets_filter_on_match)
                pass = hit;
            else if constexpr (Rules::keeps_rows_without_match)
                pass = 1 - hit;
            filter[i] = static_cast<UInt8>(pass);
            matched[matched_count] = i;
            matched_count += pass;
        }
        if constexpr (Features::need_replication)
        {
            offset += out_rows;
            offsets[i] = offset;
        }
        if constexpr (Rules::adds_row_ref && !keeps_words_in_place)
        {
            kept_words[kept] = kept_word;
            kept += default_row ? 1 : hit;
        }
        else if constexpr (default_row && !keeps_words_in_place)
        {
            kept_words[kept] = 0;
            kept += 1 - hit;
        }
        row_count += out_rows;
        matches += hit_rows;
    }

    ALWAYS_INLINE void addMatch(size_t i [[maybe_unused]], MatchWord word [[maybe_unused]])
    {
        if constexpr (need_filter && Rules::sets_filter_on_match)
        {
            filter[i] = 1;
            matched[matched_count++] = i;
        }
        size_t hit_rows = 0;
        if constexpr (Rules::adds_row_ref)
        {
            hit_rows = Rules::on_match == OnMatch::AllRowsOfKey ? refWordRows(word) : 1;
            if (record_row_refs)
            {
                if constexpr (Rules::on_match == OnMatch::FirstRowOfKey && maps_all)
                    word = RowRefList::fromWord(word).firstWord();
                kept_words[kept++] = word;
                row_count += hit_rows;
                matches += hit_rows;
            }
        }
        if constexpr (Features::need_replication)
        {
            offset += hit_rows;
            offsets[i] = offset;
        }
    }

    JoinRightColumns & join;
    const size_t first_row;
    MatchWord * kept_words;
    const bool record_row_refs;
    UInt8 * offset_flags = nullptr;
    UInt8 * filter = nullptr;
    UInt64 * matched = nullptr;
    IColumn::Offset * offsets = nullptr;
    const IColumn * asof_key = nullptr;
    size_t matched_count = 0;
    size_t kept = 0;
    size_t row_count = 0;
    size_t matches = 0;
    IColumn::Offset offset = 0;
};

/// With `words_in_row_refs`: the key search and the match step on search batches (see `max_search_batch_rows`). One adder
/// adds the matches of the whole block, and keeps at most one row ref per row at the front of `match_words`. The key search
/// of the next search batch writes its match words further on, from `match_words + begin`, so it does not overwrite the row
/// refs.
/// `prefetch_flags` is for a block that the ring takes, in a join with a used flag per key.
template <typename Rules, bool need_filter, bool prefetch_flags>
NO_INLINE size_t findKeysInBatchesAndAddMatches(JoinRightColumns & join)
{
    constexpr bool sets_flags = Rules::used_flag_action != UsedFlagAction::None;
    static_assert(sets_flags || !prefetch_flags);
    MatchAdder<Rules, need_filter, /*branch_free=*/true> adder(join);
    const MatchWord * words = join.match_words;
    const size_t * key_offsets = join.key_offsets.data();
    [[maybe_unused]] const UInt8 * flags = prefetch_flags ? join.used_flags.offsetFlags() : nullptr;
    const size_t rows = join.batch_rows;
    const size_t search_batch_rows = join.search_batch_rows;
    chassert(!prefetch_flags || join.key_offsets.size() >= search_batch_rows + amac_flag_prefetch_rows);
    for (size_t begin = 0; begin < rows; begin += search_batch_rows)
    {
        const size_t size = std::min(search_batch_rows, rows - begin);
        join.findKeysOfSearchBatch(begin, size);
        for (size_t j = 0; j < size; ++j)
        {
            /// A flag is indexed by the cell of its key, so in a table larger than the cache, reading the flag misses the
            /// cache. The prefetch is for reading, because the adder reads a flag first and writes it only if the flag is 0.
            /// The key offsets past the search batch are 0 or those of the search batch before, so they also index a flag.
            if constexpr (prefetch_flags)
                __builtin_prefetch(flags + key_offsets[j + amac_flag_prefetch_rows], /*read*/ 0, /*locality*/ 3);
            adder.add(begin + j, words[begin + j], sets_flags ? key_offsets[j] : 0);
        }
    }
    return adder.finish();
}

/// Several disjuncts: adds the matches of the disjuncts of a row, in order. The used flags are per right row, so the match
/// word is enough to reach them. A right row that two disjuncts reach is added once. Stops before a row once the output has
/// reached the row limit, and returns how many rows of the batch it finished.
template <typename Rules, bool need_filter>
NO_INLINE size_t addMatchesOfDisjuncts(JoinRightColumns & join)
{
    using Features = typename Rules::Features;
    using OnMatch = typename Rules::OnMatch;
    constexpr bool maps_all = std::is_same_v<typename Rules::Maps, HashJoin::MapsAll>;
    static_assert(Rules::on_match != OnMatch::AsofRow, "An ASOF join has one disjunct");

    AddedColumns & added_columns = join.added_columns;
    JoinStuff::JoinUsedFlags & used_flags = join.used_flags;
    const size_t batch_position = join.batch_first_row - join.first_row;
    if (batch_position == 0)
    {
        if constexpr (need_filter)
        {
            added_columns.filter = IColumn::Filter(join.rows, 0);
            added_columns.matched_rows.reserve(join.rows);
        }
        if constexpr (Features::need_replication)
        {
            added_columns.offsets_to_replicate.clear();
            added_columns.offsets_to_replicate.reserve(join.rows);
        }
    }

    const size_t num_disjuncts = join.tables.size();
    const size_t batch_rows = join.batch_rows;
    const MatchWord * words = join.match_words;
    const size_t max_joined_rows = join.max_joined_rows;
    IColumn::Offset current_offset = join.added_rows;
    /// Only a join that replicates rows moves the output offset, so for the others the limit is checked once.
    const size_t rows_to_add = Features::need_replication || current_offset < max_joined_rows ? batch_rows : 0;
    [[maybe_unused]] IColumn::Offset * offsets = nullptr;
    if constexpr (Features::need_replication)
    {
        added_columns.offsets_to_replicate.resize(batch_position + rows_to_add);
        offsets = added_columns.offsets_to_replicate.data() + batch_position;
    }
    /// A local, so that the stores of the loop do not make it reload the flag.
    [[maybe_unused]] const bool record_row_refs = added_columns.record_row_refs;
    KnownRowsHolder<true> known_rows;
    size_t j = 0;
    for (; j < rows_to_add && (!Features::need_replication || current_offset < max_joined_rows); ++j)
    {
        const size_t row = batch_position + j;
        bool right_row_found = false;
        for (size_t k = 0; k < num_disjuncts; ++k)
        {
            const MatchWord word = words[k * batch_rows + j];
            if (!word)
                continue;

            right_row_found = true;
            /// The flags of the right rows of a match: every row of a `RowRefList`, or the one row of a `RowRef`.
            if constexpr (Rules::used_flag_action == UsedFlagAction::SetUsed && maps_all)
            {
                for (const UInt64 ref_word : refsOf(word))
                    used_flags.template setUsed<true, true>(refWordBlockNo(ref_word), refWordRowNo(ref_word), 0);
            }
            else if constexpr (Rules::used_flag_action == UsedFlagAction::SetUsed)
                used_flags.template setUsed<true, true>(refWordBlockNo(word), refWordRowNo(word), 0);

            if constexpr (Rules::on_match == OnMatch::AllRowsOfKey)
            {
                /// RIGHT ANY and RIGHT SEMI claim each right row they add: several keys can reach it, and only one left row
                /// may emit it.
                JoinStuff::JoinUsedFlags * claim_flags = Rules::used_flag_action == UsedFlagAction::SetUsedOnce ? &used_flags : nullptr;
                /// The call takes the address of `offset`, so `current_offset` can stay in a register.
                IColumn::Offset offset = current_offset;
                const bool added
                    = addFoundRowAll(RowRefList::fromWord(word), added_columns, offset, known_rows, claim_flags, k + 1 == num_disjuncts);
                current_offset = offset;
                if (Rules::used_flag_action != UsedFlagAction::SetUsedOnce || added)
                    setUsed<need_filter>(added_columns.filter, row, added_columns.matched_rows);
            }
            else if constexpr (Rules::on_match == OnMatch::FirstRowOfKey)
            {
                const UInt64 first = maps_all ? RowRefList::fromWord(word).firstWord() : word;
                /// INNER ANY: the first left row to claim the first right row of the key takes the key.
                bool claimed = true;
                if constexpr (Rules::used_flag_action == UsedFlagAction::SetUsedOnce)
                    claimed = used_flags.template setUsedOnce<true, true>(refWordBlockNo(first), refWordRowNo(first), 0);
                if (claimed)
                {
                    setUsed<need_filter>(added_columns.filter, row, added_columns.matched_rows);
                    added_columns.appendFromBlock(first);
                }
            }
            else if constexpr (Rules::on_match == OnMatch::LeftRowOnly)
                setUsed<need_filter>(added_columns.filter, row, added_columns.matched_rows);

            if constexpr (Rules::stops_at_first_match)
                break;
        }

        if (!right_row_found)
        {
            if constexpr (Rules::keeps_rows_without_match)
                setUsed<need_filter>(added_columns.filter, row, added_columns.matched_rows);
            if constexpr (Features::add_missing)
            {
                if (record_row_refs)
                    added_columns.lazy_output.addDefault();
                if constexpr (Features::need_replication)
                    ++current_offset;
            }
        }
        else
            known_rows.clear();

        if constexpr (Features::need_replication)
            offsets[j] = current_offset;
    }
    if constexpr (Features::need_replication)
        added_columns.offsets_to_replicate.resize(batch_position + j);
    join.added_rows = current_offset;
    return j;
}

/// A mixed ON expression: first collects all the right rows that match a left row of the batch on the keys, then filters out
/// the pairs for which the additional filter expression is not true. Stops before a row once the pairs of the block have
/// reached the row limit, and returns how many rows of the batch it finished.
template <typename Rules, bool need_filter>
NO_INLINE size_t addMatchesWithAdditionalFilter(JoinRightColumns & join)
{
    using Features = typename Rules::Features;
    static_assert(Features::is_maps_all);

    AddedColumns & added_columns = join.added_columns;
    JoinStuff::JoinUsedFlags & used_flags = join.used_flags;
    const size_t num_disjuncts = join.tables.size();
    const bool flag_per_row = Features::right || Features::full || num_disjuncts > 1;

    const size_t batch_position = join.batch_first_row - join.first_row;
    if (batch_position == 0)
    {
        if constexpr (need_filter)
        {
            added_columns.filter = IColumn::Filter(join.rows, 0);
            added_columns.matched_rows.reserve(join.rows);
        }

        if constexpr (Features::need_replication)
            added_columns.offsets_to_replicate = IColumn::Offsets(join.rows);
    }

    /// One ref per right row, so the additional filter can accept or reject each row.
    class PreSelectedRows
    {
    public:
        explicit PreSelectedRows(PODArray<UInt64> & container_) : container(container_) {}
        void appendFromBlock(UInt64 ref_word) { container.push_back(ref_word); }

        PODArray<UInt64> & container;
    };

    PODArray<UInt64> selected_rows;
    selected_rows.reserve(join.batch_rows);

    IColumn::Offsets row_replicate_offset;
    row_replicate_offset.reserve(join.batch_rows);

    {
        IColumn::Offset current_added_rows = 0;
        PreSelectedRows selected_rows_view{selected_rows};
        for (size_t i = 0; i < join.batch_rows && join.added_rows + current_added_rows < join.max_joined_rows; ++i)
        {
            KnownRowsHolder<true> all_flag_known_rows;
            KnownRowsHolder<false> single_flag_known_rows;
            for (size_t k = 0; k < num_disjuncts; ++k)
            {
                const MatchWord word = join.matchWordsOf(k)[i];
                if (!word)
                    continue;
                const RowRefList mapped = RowRefList::fromWord(word);
                const bool is_last_disjunct = k + 1 == num_disjuncts;
                if (flag_per_row)
                    addFoundRowAll(mapped, selected_rows_view, current_added_rows, all_flag_known_rows, nullptr, is_last_disjunct);
                else
                    addFoundRowAll(mapped, selected_rows_view, current_added_rows, single_flag_known_rows, nullptr, is_last_disjunct);
            }
            row_replicate_offset.push_back(current_added_rows);
        }

        if (selected_rows.size() != current_added_rows)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Sizes are mismatched. selected_rows.size:{}, current_added_rows:{}, row_replicate_offset.size:{}",
                selected_rows.size(),
                current_added_rows,
                row_replicate_offset.size());
        join.added_rows += current_added_rows;
    }

    const size_t left_rows = row_replicate_offset.size();
    IColumn::Offset total_added_rows = 0;
    if constexpr (Features::need_replication)
        total_added_rows = batch_position ? added_columns.offsets_to_replicate[batch_position - 1] : 0;

    {
        auto filter_col = buildAdditionalFilter(join.batch_first_row, selected_rows, row_replicate_offset, added_columns);

        const PaddedPODArray<UInt8> & filter_flags = assert_cast<const ColumnUInt8 &>(*filter_col).getData();

        if (added_columns.match_stats) [[unlikely]]
            for (size_t row = 0; row < selected_rows.size(); ++row)
                if (filter_flags[row])
                    added_columns.match_stats->markRightMatched(selected_rows[row]);

        [[maybe_unused]] UInt64 matched_left = 0;
        size_t prev_replicated_row = 0;
        auto * selected_right_row_it = selected_rows.begin();
        for (size_t i = 0; i < left_rows; ++i)
        {
            const size_t row = batch_position + i;
            bool any_matched = false;
            /// right/full join or multiple disjuncts, we need to mark used flags for each row.
            if (flag_per_row)
            {
                for (size_t replicated_row = prev_replicated_row; replicated_row < row_replicate_offset[i]; ++replicated_row)
                {
                    if (filter_flags[replicated_row])
                    {
                        const UInt64 selected_ref = *selected_right_row_it;
                        if constexpr (Features::is_semi_join || Features::is_any_join)
                        {
                            /// For LEFT/INNER SEMI/ANY JOIN, we need to add only first appeared row from left,
                            if constexpr (Features::left || Features::inner)
                            {
                                if (!any_matched)
                                {
                                    // For inner join, we need mark each right row'flag, because we only use each right row once.
                                    auto used_once = used_flags.template setUsedOnce<Features::need_flags, true>(
                                        refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                                    if (used_once)
                                    {
                                        any_matched = true;
                                        total_added_rows += 1;
                                        added_columns.appendFromBlock(selected_ref);
                                    }
                                }
                            }
                            else
                            {
                                auto used_once = used_flags.template setUsedOnce<Features::need_flags, true>(
                                    refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                                if (used_once)
                                {
                                    any_matched = true;
                                    total_added_rows += 1;
                                    added_columns.appendFromBlock(selected_ref);
                                }
                            }
                        }
                        else if constexpr (Features::is_anti_join)
                        {
                            any_matched = true;
                            if constexpr (Features::right && Features::need_flags)
                                used_flags.template setUsed<true, true>(refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                        }
                        else
                        {
                            any_matched = true;
                            total_added_rows += 1;
                            added_columns.appendFromBlock(selected_ref);
                            used_flags.template setUsed<Features::need_flags, true>(
                                refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                        }
                    }

                    ++selected_right_row_it;
                }
            }
            else
            {
                for (size_t replicated_row = prev_replicated_row; replicated_row < row_replicate_offset[i]; ++replicated_row)
                {
                    if constexpr (Features::is_anti_join)
                    {
                        any_matched |= filter_flags[replicated_row];
                    }
                    else if constexpr (Features::need_replication)
                    {
                        if (filter_flags[replicated_row])
                        {
                            any_matched = true;
                            added_columns.appendFromBlock(*selected_right_row_it);
                            total_added_rows += 1;
                        }
                        ++selected_right_row_it;
                    }
                    else
                    {
                        if (filter_flags[replicated_row])
                        {
                            any_matched = true;
                            added_columns.appendFromBlock(*selected_right_row_it);
                            total_added_rows += 1;
                            selected_right_row_it = selected_right_row_it + row_replicate_offset[i] - replicated_row;
                            break;
                        }
                        ++selected_right_row_it;
                    }
                }
            }


            if constexpr (Features::is_anti_join)
            {
                if (!any_matched)
                {
                    if constexpr (Features::left)
                        if (need_filter)
                            setUsed<true>(added_columns.filter, row, added_columns.matched_rows);
                    addNotFoundRow<Features::add_missing, Features::need_replication>(added_columns, total_added_rows);
                }
            }
            else
            {
                if (!any_matched)
                {
                    addNotFoundRow<Features::add_missing, Features::need_replication>(added_columns, total_added_rows);
                }
                else
                {
                    if constexpr (Features::need_flags)
                        if (!flag_per_row)
                            used_flags.template setUsed<true, false>(0, 0, join.key_offsets[i]);
                    if (need_filter)
                        setUsed<true>(added_columns.filter, row, added_columns.matched_rows);
                }
            }
            if constexpr (leftMatchedSource(Rules::kind, Rules::strictness) == LeftMatchedSource::DefaultRowMarkers)
                matched_left += any_matched;

            if constexpr (Features::need_replication)
            {
                added_columns.offsets_to_replicate[row] = total_added_rows;
            }
            prev_replicated_row = row_replicate_offset[i];
        }

        if constexpr (leftMatchedSource(Rules::kind, Rules::strictness) == LeftMatchedSource::DefaultRowMarkers)
            added_columns.matched_left_rows += matched_left;
    }

    /// After the last batch of the call, trim the filter to the rows the call processed. The loop above may stop early at
    /// `max_joined_block_rows`, and the left block is cut to the processed rows downstream. The required right key column is
    /// built from the filter, so it must have the same rows as the cut left block.
    if (left_rows < join.batch_rows || batch_position + join.batch_rows == join.rows)
    {
        const size_t left_block_rows = batch_position + left_rows;
        if constexpr (Features::need_replication)
        {
            added_columns.offsets_to_replicate.resize(left_block_rows);
            added_columns.filter.resize(left_block_rows);
        }
        else if (need_filter)
            added_columns.filter.resize(left_block_rows);
    }
    return left_rows;
}

/// Finds the keys and adds the output of each row in one loop, so that nothing is stored and loaded again. There is one
/// instance per key type and join kind, so it is used only where splitting the two steps is slower.
template <HashJoin::Type TYPE, typename Maps, bool fast_path, typename Rules, bool need_filter, typename Lookup>
NO_INLINE size_t findKeysAndAddMatches(JoinRightColumns & join)
{
    static_assert(Lookup::retires_rows_in_order, "`MatchAdder` adds rows in row order");
    MatchAdder<Rules, need_filter, /*branch_free=*/false> adder(join);
    Lookup::template run<TYPE, Maps, fast_path, /*row_list=*/false>(
        {*join.tables[0],
         join.added_columns.join_on_keys[0],
         join.rowsToProbe(0),
         join.added_columns.enable_prefetch,
         join.batch_first_row,
         nullptr,
         nullptr,
         join.batch_rows},
        [&](size_t i, size_t, const auto & result) __attribute__((always_inline))
        {
            if (!result.isFound())
                return adder.addMiss(i);
            size_t key_offset = 0;
            if constexpr (Rules::used_flag_action != UsedFlagAction::None)
                key_offset = result.getOffset();
            adder.addFound(i, key_offset, [&] __attribute__((always_inline)) { return matchWordOf<Maps>(result); });
        });
    return adder.finish();
}

template <typename Rules, bool need_filter, bool fast_path>
AddMatchesFn selectFused(HashJoin::Type key_type)
{
    switch (key_type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        return &findKeysAndAddMatches<HashJoin::Type::TYPE, typename Rules::Maps, fast_path, Rules, need_filter, PrefetchingLookup>;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
    UNREACHABLE();
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
JoinRightColumns::Functions selectFunctions(HashJoin::Type key_type, size_t num_disjuncts, bool has_additional_filter, bool record_row_refs)
{
    using Rules = MatchRules<KIND, STRICTNESS, Maps>;
    using Features = typename Rules::Features;

    JoinRightColumns::Functions functions;
    /// In the lambdas below, `later` is true for the disjuncts after the first.
    /// The key search by the AMAC ring, in two copies: for a table whose buckets are not in the Robin Hood mode, and for one
    /// whose buckets are.
    const auto select_find_keys_ring = [&](bool later, FindKeysMode mode)
    {
        functions.find_keys_ring[0][later][0] = selectFindKeys<Maps, AmacLookup<false>>(key_type, mode, /*fast_path=*/false);
        functions.find_keys_ring[0][later][1] = selectFindKeys<Maps, AmacLookup<false>>(key_type, mode, /*fast_path=*/true);
        functions.find_keys_ring[1][later][0] = selectFindKeys<Maps, AmacLookup<true>>(key_type, mode, /*fast_path=*/false);
        functions.find_keys_ring[1][later][1] = selectFindKeys<Maps, AmacLookup<true>>(key_type, mode, /*fast_path=*/true);
    };
    const auto select_find_keys_in_row_order = [&](bool later, FindKeysMode mode)
    {
        functions.find_keys[later][0] = selectFindKeys<Maps, PrefetchingLookup>(key_type, mode, /*fast_path=*/false);
        functions.find_keys[later][1] = selectFindKeys<Maps, PrefetchingLookup>(key_type, mode, /*fast_path=*/true);
    };
    const auto select_find_keys = [&](bool later, FindKeysMode mode)
    {
        select_find_keys_in_row_order(later, mode);
        select_find_keys_ring(later, mode);
    };

    /// A join kind with `JoinFeatures::need_filter` always sets `AddedColumns::need_filter`, so it needs no copy of a function
    /// for `need_filter` = false.
    if (has_additional_filter)
    {
        if constexpr (Features::is_maps_all)
        {
            /// The key search of the first disjunct stops once the pairs it found reach the row limit: a later disjunct
            /// only adds pairs.
            functions.records_key_offsets = Features::need_flags && !Features::right && !Features::full && num_disjuncts == 1;
            select_find_keys(
                false,
                functions.records_key_offsets ? FindKeysMode::WordsAndKeyOffsetsUpToRowLimit : FindKeysMode::WordsUpToRowLimit);
            select_find_keys(true, FindKeysMode::Words);
            /// The whole block, unless the row limit stops a call early: the filter runs once per batch.
            functions.max_batch_rows = std::numeric_limits<size_t>::max();
            if constexpr (!Features::need_filter)
                functions.add_matches[0] = &addMatchesWithAdditionalFilter<Rules, false>;
            functions.add_matches[1] = &addMatchesWithAdditionalFilter<Rules, true>;
            return functions;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Additional filter expression is not supported for this JOIN");
    }

    if (num_disjuncts > 1)
    {
        if constexpr (!Features::is_asof_join)
        {
            /// With replication, the key search of the first disjunct stops once the right rows it found reach the row limit.
            /// The match step adds at least that many rows for those left rows, unless it skips right rows that another left
            /// row claimed. That only makes the batch end before its output reaches the limit.
            select_find_keys(false, Features::need_replication ? FindKeysMode::WordsUpToRowLimit : FindKeysMode::Words);
            select_find_keys(true, Rules::stops_at_first_match ? FindKeysMode::WordsOfRowList : FindKeysMode::Words);
            /// Several disjuncts have no required right keys (see the `HashJoin` constructor), so only the copy with the join
            /// kind's own `need_filter` can run.
            functions.add_matches[Features::need_filter] = &addMatchesOfDisjuncts<Rules, Features::need_filter>;
            functions.max_batch_rows = max_disjunct_batch_rows;
            functions.stops_at_first_match = Rules::stops_at_first_match;
            return functions;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected ASOF join with {} disjuncts", num_disjuncts);
    }

    functions.words_in_row_refs = true;
    constexpr bool sets_flags = Rules::used_flag_action != UsedFlagAction::None;
    functions.records_key_offsets = sets_flags;
    const FindKeysMode mode = sets_flags ? FindKeysMode::WordsAndKeyOffsets : FindKeysMode::Words;
    /// A block that the ring takes runs in two steps, also for a fused join: the ring retires rows out of order.
    select_find_keys_ring(false, mode);
    if constexpr (!Features::need_filter)
        functions.ring_batches[0] = &findKeysInBatchesAndAddMatches<Rules, false, /*prefetch_flags=*/sets_flags>;
    functions.ring_batches[1] = &findKeysInBatchesAndAddMatches<Rules, true, /*prefetch_flags=*/sets_flags>;

    if constexpr (!Features::is_asof_join)
    {
        /// Without row refs to record, a separate match step only costs time.
        if (Rules::fused_with_one_disjunct || !record_row_refs)
        {
            if constexpr (!Features::need_filter)
            {
                functions.fused[0][0] = selectFused<Rules, false, false>(key_type);
                functions.fused[0][1] = selectFused<Rules, false, true>(key_type);
            }
            functions.fused[1][0] = selectFused<Rules, true, false>(key_type);
            functions.fused[1][1] = selectFused<Rules, true, true>(key_type);
            return functions;
        }
    }

    if constexpr (!Features::need_filter)
        functions.add_matches[0] = &findKeysInBatchesAndAddMatches<Rules, false, /*prefetch_flags=*/false>;
    functions.add_matches[1] = &findKeysInBatchesAndAddMatches<Rules, true, /*prefetch_flags=*/false>;
    select_find_keys_in_row_order(false, mode);
    return functions;
}
}

}
