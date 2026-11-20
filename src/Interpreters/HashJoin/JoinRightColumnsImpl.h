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
#include <Interpreters/HashJoin/PrepareKeys.h>

#include <type_traits>

/// The single pass and the match steps of the hash join probe, `selectFunctions` and `probeBlock`.
namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// `addFoundRowAll` with a used flag per right row. Not inlined: when it is inlined, the row loops of the match steps spill
/// registers, also for the rows without a match.
template <typename AddedColumns>
NO_INLINE bool addFoundRowAllWithFlagPerRow(
    const RowRefList & mapped,
    AddedColumns & added,
    IColumn::Offset & current_offset,
    KnownRowsHolder<true> & known_rows,
    JoinStuff::JoinUsedFlags * claim_flags,
    bool is_last_disjunct,
    std::vector<UInt64> & new_known_rows)
{
    new_known_rows.clear();
    bool any_row_added = false;

    for (const UInt64 ref_word : refsOf(mapped.word))
    {
        if (known_rows.isKnown(ref_word))
            continue;

        if (claim_flags && !claim_flags->setUsedOnce<true>(refWordBlockNo(ref_word), refWordRowNo(ref_word)))
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
    bool is_last_disjunct [[maybe_unused]],
    std::vector<UInt64> & new_known_rows [[maybe_unused]])
{
    if constexpr (flag_per_row)
        return addFoundRowAllWithFlagPerRow(mapped, added, current_offset, known_rows, claim_flags, is_last_disjunct, new_known_rows);
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
                    size_t rows = row_replicate_offset[i] - row_replicate_offset[static_cast<ssize_t>(i) - 1];
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

/// What a match does to the used flags: with one disjunct to the flag of its key, with several to the flags of its right rows.
enum class UsedFlagAction : uint8_t
{
    None,
    /// RIGHT and FULL ALL, RIGHT and FULL `RightAny`, RIGHT ANTI: the scan after the probe emits the right rows nobody used.
    SetUsed,
    /// INNER ANY, RIGHT ANY, RIGHT SEMI: the first left row to set the flag takes the key. For the other rows it is no match.
    SetUsedOnce,
};

/// What a join does with a row whose key was found.
enum class OnMatch : uint8_t
{
    /// ALL, and RIGHT ANY or RIGHT SEMI once the row took the key: the left row is replicated for every row of the key.
    AllRowsOfKey,
    /// LEFT and INNER ANY, SEMI, `RightAny`.
    FirstRowOfKey,
    /// The row that `findAsof` picks, or no match if there is none.
    AsofRow,
    /// A match on a set map can keep the left row without reading a right row.
    LeftRowOnly,
    /// ANTI, and FULL ANY, which the probe does not implement.
    NoOutput,
};

/// What the pass of a join with one disjunct does with a row whose key was found and with one whose key was not. Two join
/// kinds with the same handling share one pass per key type.
template <typename MapsT, UsedFlagAction used_flag_action_, OnMatch on_match_, bool add_missing_, bool need_replication_,
          bool keeps_rows_without_match_, bool filters_always_>
struct MatchHandling
{
    using Maps = MapsT;
    static constexpr UsedFlagAction used_flag_action = used_flag_action_;
    static constexpr OnMatch on_match = on_match_;
    static constexpr bool add_missing = add_missing_;
    static constexpr bool need_replication = need_replication_;
    static constexpr bool keeps_rows_without_match = keeps_rows_without_match_;
    /// The join kind always has a filter (`JoinFeatures::need_filter`). Otherwise `AddedColumns::need_filter` tells it per block.
    static constexpr bool filters_always = filters_always_;
};

/// What a join kind does with a match and with a row that has none, derived from `JoinFeatures`.
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
struct MatchRules
{
    using Features = JoinFeatures<KIND, STRICTNESS, MapsTemplate>;
    using Maps = MapsTemplate;
    static constexpr JoinKind kind = KIND;
    static constexpr JoinStrictness strictness = STRICTNESS;

    static constexpr OnMatch on_match = []
    {
        if constexpr (SetJoinMaps<Maps>)
            return Features::is_anti_join ? OnMatch::NoOutput : OnMatch::LeftRowOnly;
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
        if constexpr (!Features::need_flags || Features::is_asof_join || (Features::is_any_join && Features::full))
            return UsedFlagAction::None;
        else if constexpr (
            (Features::is_any_join && Features::inner) || ((Features::is_any_join || Features::is_semi_join) && Features::right))
            return UsedFlagAction::SetUsedOnce;
        else if constexpr (Features::is_anti_join)
            return Features::right ? UsedFlagAction::SetUsed : UsedFlagAction::None;
        else
            return UsedFlagAction::SetUsed;
    }();

    static constexpr bool keeps_rows_without_match = Features::is_anti_join && Features::left;
    static constexpr bool stops_at_first_match
        = Features::is_any_or_semi_join && !(Features::is_any_join && (Features::right || Features::full));

    using Handling = MatchHandling<Maps, used_flag_action, on_match, Features::add_missing, Features::need_replication,
        keeps_rows_without_match, Features::need_filter>;
};

enum class MatchLayout : uint8_t
{
    InOrder,    /// The lookup visits left rows in order.
    Fixed,      /// Every input has one output slot, including left outer misses.
    Selected,   /// One left index for each emitted pair or group.
    Indexed,    /// The postpass consumes a match word at each left index.
    MarkOnly,
};

template <typename Handling, MatchLayout layout>
class MatchAdder
{
    static constexpr bool ordered = layout == MatchLayout::InOrder;
    static constexpr bool indexed = layout == MatchLayout::Indexed;
    static constexpr bool selected = layout == MatchLayout::Selected;

public:
    ALWAYS_INLINE explicit MatchAdder(JoinRightColumnsData & data_)
        : data(data_)
        , record_row_refs(data.added_columns.record_row_refs)
        , need_filter(Handling::filters_always || data.added_columns.need_filter)
    {
        auto & added = data.added_columns;
        auto & output = added.lazy_output;
        const size_t rows = data.batch_rows;
        if (layout != MatchLayout::MarkOnly && (record_row_refs || indexed))
        {
            output.row_refs.resize(rows);
            words = output.row_refs.data();
        }
        if ((ordered || layout == MatchLayout::Fixed) && need_filter)
        {
            if constexpr (ordered)
                added.filter.resize_fill(rows, 0);
            else
                added.filter.resize(rows);
            filter = added.filter.data();
        }
        if (selected || (ordered && need_filter))
        {
            added.matched_rows.resize(rows);
            matched = added.matched_rows.data();
        }
        if constexpr (!indexed && Handling::need_replication)
        {
            added.offsets_to_replicate.resize(rows);
            offsets = added.offsets_to_replicate.data();
        }
        if constexpr (Handling::used_flag_action != UsedFlagAction::None)
            flags = data.used_flags.offsetFlags();
        if constexpr (Handling::on_match == OnMatch::AsofRow)
            asof_key = &added.leftAsofKey();
        if constexpr (selected)
            output.probe_output = ProbeOutput::Selected;
    }

    template <typename Result>
    ALWAYS_INLINE void addFound(size_t i, size_t source_row [[maybe_unused]], const Result & result)
    {
        if constexpr (Handling::used_flag_action == UsedFlagAction::SetUsedOnce)
        {
            if (!JoinStuff::JoinUsedFlags::setUsedOnceAt(flags, result.getOffset()))
                return addMiss(i);
        }
        else if constexpr (Handling::used_flag_action == UsedFlagAction::SetUsed)
            JoinStuff::JoinUsedFlags::setUsedAt(flags, result.getOffset());

        if constexpr (Handling::on_match == OnMatch::AsofRow)
        {
            const RowRef * row = result.getMapped()->findAsof(*asof_key, source_row);
            if (!row)
                return addMiss(i);
            write<true>(i, row->encode());
        }
        else if constexpr (Handling::on_match == OnMatch::FirstRowOfKey)
            write<true>(i, record_row_refs ? firstRefWord(result.getMapped()) : 1);
        else if constexpr (Handling::on_match == OnMatch::AllRowsOfKey)
            write<true>(i, result.getMapped().word);
        else
            write<true>(i, 1);
    }

    ALWAYS_INLINE void addMiss(size_t i)
    {
        write<false>(i, 0);
    }

    ALWAYS_INLINE size_t finish()
    {
        auto & added = data.added_columns;
        if constexpr (indexed)
            return data.functions.amac_postpass(data);

        if (selected || (ordered && need_filter))
            added.matched_rows.resize_assume_reserved(selected_rows);
        if constexpr (selected && Handling::need_replication)
            added.offsets_to_replicate.resize_assume_reserved(selected_rows);
        if constexpr (layout == MatchLayout::Fixed)
            kept = data.batch_rows;
        if (record_row_refs)
        {
            added.lazy_output.row_refs.resize_assume_reserved(kept);
            added.lazy_output.row_count = Handling::need_replication ? total : kept;
            added.lazy_output.hash_table_matches = Handling::add_missing ? matches : added.lazy_output.row_count;
#ifndef NDEBUG
            for (MatchWord word : added.lazy_output.row_refs)
                if (word)
                    added.checkColumnsOfRef(word);
#endif
        }
        added.lazy_output.output_by_row_list = Handling::need_replication;
        return data.batch_rows;
    }

private:
    template <bool hit>
    ALWAYS_INLINE void write(size_t i [[maybe_unused]], MatchWord word [[maybe_unused]])
    {
        constexpr bool keep = hit ? Handling::on_match != OnMatch::NoOutput : Handling::add_missing || Handling::keeps_rows_without_match;
        if constexpr (layout == MatchLayout::MarkOnly)
            static_assert(!keep);
        else if constexpr (indexed)
        {
            /// A kept anti-join row has no right ref. Use a nonzero token until offsets are built.
            if constexpr (Handling::keeps_rows_without_match)
                words[i] = keep;
            else
                words[i] = keep ? word : 0;
        }
        else if constexpr (layout == MatchLayout::Fixed)
        {
            static_assert(keep);
            if (need_filter)
                filter[i] = hit;
            if (record_row_refs)
            {
                words[i] = word;
                matches += hit;
            }
        }
        else
        {
            if constexpr (selected && keep)
                matched[selected_rows++] = i;
            else if constexpr (ordered && (hit ? keep : Handling::keeps_rows_without_match))
            {
                if (need_filter)
                {
                    filter[i] = 1;
                    matched[selected_rows++] = i;
                }
            }
            if constexpr (keep)
            {
                const size_t copies = hit && Handling::need_replication ? refWordRows(word) : 1;
                if constexpr (Handling::need_replication)
                    total += copies;
                if (record_row_refs)
                {
                    words[kept++] = word;
                    if constexpr (hit && Handling::add_missing)
                        matches += copies;
                }
                if constexpr (selected && Handling::need_replication)
                    offsets[selected_rows - 1] = total;
            }
            if constexpr (ordered && Handling::need_replication)
                offsets[i] = total;
        }
    }

    JoinRightColumnsData & data;
    const bool record_row_refs;
    const bool need_filter;
    UInt8 * flags = nullptr;
    const IColumn * asof_key = nullptr;
    MatchWord * words = nullptr;
    UInt8 * filter = nullptr;
    UInt64 * matched = nullptr;
    IColumn::Offset * offsets = nullptr;
    size_t selected_rows = 0;
    size_t kept = 0;
    size_t total = 0;
    size_t matches = 0;
};

/// Several disjuncts: adds the matches of the disjuncts of a row, in order. The used flags are per right row, so the match
/// word is enough to reach them. A right row that two disjuncts reach is added once. Stops before a row once the output has
/// reached the row limit, and returns how many rows of the batch it finished.
template <typename Rules>
NO_INLINE size_t addMatchesOfDisjuncts(JoinRightColumnsData & data)
{
    using Features = typename Rules::Features;
    constexpr bool need_filter = Features::need_filter;
    constexpr bool maps_all = std::is_same_v<typename Rules::Maps, HashJoin::MapsAll>;
    static_assert(Rules::on_match != OnMatch::AsofRow, "An ASOF join has one disjunct");

    AddedColumns & added_columns = data.added_columns;
    JoinStuff::JoinUsedFlags & used_flags = data.used_flags;
    const size_t batch_position = data.batch_first_row - data.first_row;
    if (batch_position == 0)
    {
        if constexpr (need_filter)
        {
            added_columns.filter = IColumn::Filter(data.rows, 0);
            added_columns.matched_rows.reserve(data.rows);
        }
        if constexpr (Features::need_replication)
        {
            added_columns.offsets_to_replicate.clear();
            added_columns.offsets_to_replicate.reserve(data.rows);
        }
    }

    const size_t num_disjuncts = data.tables.size();
    const size_t batch_rows = data.batch_rows;
    const MatchWord * words = data.matchWordsOf(0);
    const size_t max_joined_rows = data.max_joined_rows;
    /// Only a join that replicates rows moves the output offset. For the others it stays 0.
    IColumn::Offset current_offset = data.added_rows;
    [[maybe_unused]] IColumn::Offset * offsets = nullptr;
    if constexpr (Features::need_replication)
    {
        added_columns.offsets_to_replicate.resize(batch_position + batch_rows);
        offsets = added_columns.offsets_to_replicate.data() + batch_position;
    }
    /// A local, so that the stores of the loop do not make it reload the flag.
    [[maybe_unused]] const bool record_row_refs = added_columns.record_row_refs;
    KnownRowsHolder<true> known_rows;
    [[maybe_unused]] std::vector<UInt64> & new_known_rows = data.left_block_state.new_known_rows;
    size_t j = 0;
    for (; j < batch_rows && (!Features::need_replication || current_offset < max_joined_rows); ++j)
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
                    used_flags.template setUsed<true>(refWordBlockNo(ref_word), refWordRowNo(ref_word));
            }
            else if constexpr (Rules::used_flag_action == UsedFlagAction::SetUsed)
                used_flags.template setUsed<true>(refWordBlockNo(word), refWordRowNo(word));

            if constexpr (Rules::on_match == OnMatch::AllRowsOfKey)
            {
                /// RIGHT ANY and RIGHT SEMI claim each right row they add: several keys can reach it, and only one left row
                /// may emit it.
                JoinStuff::JoinUsedFlags * claim_flags = Rules::used_flag_action == UsedFlagAction::SetUsedOnce ? &used_flags : nullptr;
                /// The call takes the address of `offset`, so `current_offset` can stay in a register.
                IColumn::Offset offset = current_offset;
                const bool added = addFoundRowAll(
                    RowRefList::fromWord(word), added_columns, offset, known_rows, claim_flags, k + 1 == num_disjuncts, new_known_rows);
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
                    claimed = used_flags.template setUsedOnce<true>(refWordBlockNo(first), refWordRowNo(first));
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
    data.added_rows = current_offset;
    return j;
}

/// A mixed ON expression: first collects all the right rows that match a left row of the batch on the keys, then filters out
/// the pairs for which the additional filter expression is not true. Stops before a row once the pairs of the call have
/// reached the row limit, and returns how many rows of the batch it finished.
template <typename Rules, bool need_filter>
NO_INLINE size_t addMatchesWithAdditionalFilter(JoinRightColumnsData & data)
{
    using Features = typename Rules::Features;
    static_assert(Features::is_maps_all);

    AddedColumns & added_columns = data.added_columns;
    JoinStuff::JoinUsedFlags & used_flags = data.used_flags;
    const size_t num_disjuncts = data.tables.size();
    const bool flag_per_row = Features::right || Features::full || num_disjuncts > 1;

    const size_t batch_position = data.batch_first_row - data.first_row;
    if (batch_position == 0)
    {
        if constexpr (need_filter)
        {
            added_columns.filter = IColumn::Filter(data.rows, 0);
            added_columns.matched_rows.reserve(data.rows);
        }

        if constexpr (Features::need_replication)
            added_columns.offsets_to_replicate = IColumn::Offsets(data.rows);
    }

    /// One ref per right row, so the additional filter can accept or reject each row.
    class PreSelectedRows
    {
    public:
        explicit PreSelectedRows(PODArray<UInt64> & container_) : container(container_) {}
        void appendFromBlock(UInt64 ref_word) { container.push_back(ref_word); }

        PODArray<UInt64> & container;
    };

    PODArray<UInt64> & selected_rows = data.left_block_state.selected_rows;
    selected_rows.clear();
    selected_rows.reserve(data.batch_rows);

    IColumn::Offsets & row_replicate_offset = data.left_block_state.row_replicate_offset;
    row_replicate_offset.clear();
    row_replicate_offset.reserve(data.batch_rows);

    {
        IColumn::Offset current_added_rows = 0;
        PreSelectedRows selected_rows_view{selected_rows};
        std::vector<UInt64> & new_known_rows = data.left_block_state.new_known_rows;
        for (size_t i = 0; i < data.batch_rows && data.added_rows + current_added_rows < data.max_joined_rows; ++i)
        {
            KnownRowsHolder<true> all_flag_known_rows;
            KnownRowsHolder<false> single_flag_known_rows;
            for (size_t k = 0; k < num_disjuncts; ++k)
            {
                const MatchWord word = data.matchWordsOf(k)[i];
                if (!word)
                    continue;
                const RowRefList mapped = RowRefList::fromWord(word);
                const bool is_last_disjunct = k + 1 == num_disjuncts;
                if (flag_per_row)
                    addFoundRowAll(
                        mapped, selected_rows_view, current_added_rows, all_flag_known_rows, nullptr, is_last_disjunct, new_known_rows);
                else
                    addFoundRowAll(
                        mapped, selected_rows_view, current_added_rows, single_flag_known_rows, nullptr, is_last_disjunct, new_known_rows);
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
        data.added_rows += current_added_rows;
    }

    const size_t left_rows = row_replicate_offset.size();
    IColumn::Offset total_added_rows = 0;
    if constexpr (Features::need_replication)
        total_added_rows = batch_position ? added_columns.offsets_to_replicate[batch_position - 1] : 0;

    {
        auto filter_col = buildAdditionalFilter(data.batch_first_row, selected_rows, row_replicate_offset, added_columns);

        const PaddedPODArray<UInt8> & filter_flags = assert_cast<const ColumnUInt8 &>(*filter_col).getData();

        if (added_columns.match_stats) [[unlikely]]
            for (size_t row = 0; row < selected_rows.size(); ++row)
                if (filter_flags[row])
                    added_columns.match_stats->markRightMatched(selected_rows[row]);

        [[maybe_unused]] UInt64 matched_left = 0;
        size_t prev_replicated_row = 0;
        /// Locals, so that the stores of the loop do not make it reload the arrays of the state.
        const IColumn::Offset * const replicate_offsets = row_replicate_offset.data();
        [[maybe_unused]] const size_t * const key_offsets = data.left_block_state.key_offsets.data();
        for (size_t i = 0; i < left_rows; ++i)
        {
            const size_t row = batch_position + i;
            bool any_matched = false;
            /// right/full join or multiple disjuncts, we need to mark used flags for each row.
            if (flag_per_row)
            {
                for (size_t replicated_row = prev_replicated_row; replicated_row < replicate_offsets[i]; ++replicated_row)
                {
                    if (filter_flags[replicated_row])
                    {
                        const UInt64 selected_ref = selected_rows[replicated_row];
                        if constexpr (Features::is_semi_join || Features::is_any_join)
                        {
                            /// For LEFT/INNER SEMI/ANY JOIN, we need to add only first appeared row from left,
                            if constexpr (Features::left || Features::inner)
                            {
                                if (!any_matched)
                                {
                                    // For inner join, we need mark each right row'flag, because we only use each right row once.
                                    auto used_once = used_flags.template setUsedOnce<Features::need_flags>(
                                        refWordBlockNo(selected_ref), refWordRowNo(selected_ref));
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
                                auto used_once = used_flags.template setUsedOnce<Features::need_flags>(
                                    refWordBlockNo(selected_ref), refWordRowNo(selected_ref));
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
                                used_flags.template setUsed<true>(refWordBlockNo(selected_ref), refWordRowNo(selected_ref));
                        }
                        else
                        {
                            any_matched = true;
                            total_added_rows += 1;
                            added_columns.appendFromBlock(selected_ref);
                            used_flags.template setUsed<Features::need_flags>(refWordBlockNo(selected_ref), refWordRowNo(selected_ref));
                        }
                    }
                }
            }
            else
            {
                for (size_t replicated_row = prev_replicated_row; replicated_row < replicate_offsets[i]; ++replicated_row)
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
                            added_columns.appendFromBlock(selected_rows[replicated_row]);
                            total_added_rows += 1;
                        }
                    }
                    else if (filter_flags[replicated_row])
                    {
                        if constexpr (Features::need_flags)
                            any_matched = JoinStuff::JoinUsedFlags::setUsedOnceAt(used_flags.offsetFlags(), key_offsets[i]);
                        else
                            any_matched = true;
                        if (any_matched)
                        {
                            added_columns.appendFromBlock(selected_rows[replicated_row]);
                            ++total_added_rows;
                        }
                        break;
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
            prev_replicated_row = replicate_offsets[i];
        }

        if constexpr (leftMatchedSource(Rules::kind, Rules::strictness) == LeftMatchedSource::DefaultRowMarkers)
            added_columns.matched_left_rows += matched_left;
    }

    /// After the last batch of the call, trim the filter to the rows the call processed. The loop above may stop early at
    /// `max_joined_block_rows`, and the left block is cut to the processed rows downstream. The required right key column is
    /// built from the filter, so it must have the same rows as the cut left block.
    if (left_rows < data.batch_rows || batch_position + data.batch_rows == data.rows)
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

/// One adder collects the output of every search batch in the block.
template <HashJoin::Type TYPE, bool fast_path, typename Handling, MatchLayout layout, typename Lookup>
NO_INLINE size_t findKeysAndAddMatches(JoinRightColumnsData & data)
{
    using Maps = typename Handling::Maps;
    MatchAdder<Handling, layout> adder(data);
    for (size_t begin = 0; begin < data.batch_rows; begin += data.search_batch_rows)
    {
        const size_t size = std::min(data.search_batch_rows, data.batch_rows - begin);
        const size_t first = data.batch_first_row + begin;
        DisjunctState & disjunct = data.left_block_state.disjuncts[0];
        const FindKeysArgs args{
            .table = *data.tables[0],
            .disjunct = disjunct,
            .rows = {.first = first, .rows = nullptr, .size = size},
            .enable_prefetch = data.added_columns.enable_prefetch,
            .used_flags = data.used_flags.offsetFlags()};
        if constexpr (!Lookup::retires_rows_in_order)
            if (data.functions.prepare_keys_first)
                data.functions.prepare_keys_first(args);
        const auto on_row = [&](size_t i, size_t source_row, const auto & result) ALWAYS_INLINE
        {
            if (result.isFound())
                adder.addFound(begin + i, source_row, result);
            else
                adder.addMiss(begin + i);
        };
        Lookup::template run<TYPE, Maps, fast_path, false>(args, on_row);
    }
    return adder.finish();
}

template <typename Handling, MatchLayout layout, typename Lookup>
AddMatchesFn selectFindKeysAndAddMatches(HashJoin::Type key_type, bool fast_path)
{
    const auto of_type = [&]<HashJoin::Type TYPE>() -> AddMatchesFn
    {
        if constexpr (Lookup::template supports<TYPE, typename Handling::Maps>)
            return fast_path ? &findKeysAndAddMatches<TYPE, true, Handling, layout, Lookup>
                             : &findKeysAndAddMatches<TYPE, false, Handling, layout, Lookup>;
        return nullptr;
    };
    switch (key_type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        return of_type.template operator()<HashJoin::Type::TYPE>();
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
    UNREACHABLE();
}

/// Select per block because the build can change the key type and join strictness.
template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
Functions selectFunctions(HashJoin::Type key_type, Tables tables, bool fast_path, const AddedColumns & added, size_t rows)
{
    using Rules = MatchRules<KIND, STRICTNESS, Maps>;
    using Features = typename Rules::Features;

    const size_t num_disjuncts = tables.size();
    const bool need_filter = added.need_filter;
    Functions functions;
    /// The key searches of the first disjunct, or with `later` of the disjuncts after it. A template, so that it is
    /// instantiated only where it is called. An ASOF join never calls it, so its file does not instantiate `selectFindKeys`
    /// for `MapsAsof`.
    const auto select_find_keys = [&]<typename SearchMaps = Maps>(bool later, FindKeysMode mode)
    {
        FindKeysFn & find_keys = later ? functions.find_keys_later : functions.find_keys_first;
        auto & find_keys_ring = later ? functions.find_keys_ring_later : functions.find_keys_ring_first;
        PrepareKeysFn & prepare_keys = later ? functions.prepare_keys_later : functions.prepare_keys_first;
        find_keys = selectFindKeys<SearchMaps, PrefetchingLookup>(key_type, mode, fast_path);
        /// The ring has no search that stops at the row limit. It has one copy for buckets in the Robin Hood mode.
        find_keys_ring[0] = selectFindKeys<SearchMaps, AmacLookup<false>>(key_type, mode, fast_path);
        find_keys_ring[1] = selectFindKeys<SearchMaps, AmacLookup<true>>(key_type, mode, fast_path);
        prepare_keys = selectPrepareKeys<SearchMaps>(key_type, mode == FindKeysMode::WordsOfRowList, fast_path);
    };

    /// A join kind with `JoinFeatures::need_filter` always sets `AddedColumns::need_filter`, so it needs no copy of a function
    /// for `need_filter` = false.
    if (added.additional_filter_expression)
    {
        if constexpr (Features::is_maps_all)
        {
            /// The key search of the first disjunct stops once the pairs it found reach the row limit: a later disjunct
            /// only adds pairs.
            functions.records_key_offsets
                = Features::need_flags && !Features::right && !Features::full && !Features::is_anti_join && num_disjuncts == 1;
            select_find_keys(
                false,
                functions.records_key_offsets ? FindKeysMode::WordsAndKeyOffsetsUpToRowLimit : FindKeysMode::WordsUpToRowLimit);
            select_find_keys(true, FindKeysMode::Words);
            /// The filter runs once per batch, so the batches have no cap of their own. The first batch of a call still has at
            /// most as many rows as the limit, and each later batch doubles.
            functions.max_batch_rows = std::numeric_limits<size_t>::max();
            if constexpr (Features::need_filter)
                functions.add_matches = need_filter ? &addMatchesWithAdditionalFilter<Rules, true> : nullptr;
            else
                functions.add_matches
                    = !need_filter ? &addMatchesWithAdditionalFilter<Rules, false> : &addMatchesWithAdditionalFilter<Rules, true>;
            return functions;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Additional filter expression is not supported for this JOIN");
    }

    if (num_disjuncts > 1)
    {
        if constexpr (!Features::is_asof_join && !Features::one_clause_only)
        {
            /// With replication, the key search of the first disjunct stops once the right rows it found reach the row limit.
            /// The match step adds at least that many rows for those left rows, unless it skips right rows that another left
            /// row claimed. Then the output of the batch stays below the limit, and `runProbe` goes on with the next batch.
            select_find_keys(false, Features::need_replication ? FindKeysMode::WordsUpToRowLimit : FindKeysMode::Words);
            select_find_keys(true, Rules::stops_at_first_match ? FindKeysMode::WordsOfRowList : FindKeysMode::Words);
            /// Several disjuncts have no required right keys (see the `HashJoin` constructor), so only the copy with the join
            /// kind's own `need_filter` can run.
            if (need_filter == Features::need_filter)
                functions.add_matches = &addMatchesOfDisjuncts<Rules>;
            functions.max_batch_rows = max_disjunct_batch_rows;
            functions.stops_at_first_match = Rules::stops_at_first_match;
            return functions;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected hash join with {} disjuncts", num_disjuncts);
    }

    using Handling = typename Rules::Handling;
    constexpr bool mark_only = Handling::on_match == OnMatch::NoOutput && !Handling::add_missing
        && !Handling::keeps_rows_without_match;
    constexpr bool fixed = Handling::add_missing && !Handling::need_replication
        && (Handling::on_match == OnMatch::FirstRowOfKey || Handling::on_match == OnMatch::AsofRow);

    if (rows && tables.front()->shouldUseAmacLookup(added.amac_mode))
    {
        const auto use_layout = [&]<MatchLayout layout>()
        {
            constexpr bool flags = Handling::used_flag_action != UsedFlagAction::None;
            functions.add_matches = tables.front()->bucketsAreRobinHood()
                ? selectFindKeysAndAddMatches<Handling, layout, AmacLookup<true, flags>>(key_type, fast_path)
                : selectFindKeysAndAddMatches<Handling, layout, AmacLookup<false, flags>>(key_type, fast_path);
            if constexpr (layout == MatchLayout::Indexed)
            {
                if constexpr (Features::inner && (Handling::on_match == OnMatch::FirstRowOfKey || Handling::on_match == OnMatch::LeftRowOnly))
                    functions.amac_postpass = &compactOrderedAmacRows;
                else
                    functions.amac_postpass = selectAmacPostPass(
                        Handling::need_replication, Handling::add_missing, Handling::keeps_rows_without_match);
            }
        };
        if constexpr (mark_only)
            use_layout.template operator()<MatchLayout::MarkOnly>();
        else if constexpr (fixed)
            use_layout.template operator()<MatchLayout::Fixed>();
        else if constexpr (Features::is_all_join)
            use_layout.template operator()<MatchLayout::Indexed>();
        else if (added.allow_reordering)
            use_layout.template operator()<MatchLayout::Selected>();
        else
            use_layout.template operator()<MatchLayout::Indexed>();

        if (functions.add_matches)
        {
            functions.prepare_keys_first = selectPrepareKeys<Maps>(key_type, false, fast_path);
            functions.uses_amac = true;
            return functions;
        }
    }
    functions.add_matches = selectFindKeysAndAddMatches<Handling, MatchLayout::InOrder, PrefetchingLookup>(key_type, fast_path);
    return functions;
}

/// Probes the rows of `selector` with the functions of the join kind for this block (see `runProbe`). Instantiated with the
/// methods of each join kind in its own file, such as `InnerHashJoinAll.cpp`.
template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
size_t probeBlock(
    HashJoin::Type key_type,
    Tables tables,
    JoinStuff::JoinUsedFlags & used_flags,
    AddedColumns & added_columns,
    LeftBlockState & left_block_state,
    const ScatteredBlock::Selector & selector)
{
    const auto functions = selectFunctions<KIND, STRICTNESS, Maps>(
        key_type, tables, canUseFastPath(left_block_state), added_columns, selector.size());
    return runProbe(functions, tables, used_flags, added_columns, left_block_state, selector);
}
}

}
