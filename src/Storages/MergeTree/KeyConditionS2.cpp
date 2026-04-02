#include <Storages/MergeTree/KeyConditionS2.h>

#if USE_S2_GEOMETRY

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wambiguous-reversed-operator"
#include <Functions/s2_fwd.h>
#include <s2/s2cell.h>
#include <s2/s2cell_id.h>
#include <s2/s2cell_union.h>
#include <s2/s2region_coverer.h>
#pragma clang diagnostic pop

#include <Common/FieldVisitorConvertToNumber.h>

namespace DB
{

// ---------------------------------------------------------------------------
// Internal helper
// ---------------------------------------------------------------------------

/// Returns true if any cell in `covering` has a Hilbert-curve range that
/// overlaps the granule's actual interval
///   [cell_min.range_min(), cell_max.range_max()].
///
/// More precise than S2CellUnion::Intersects(ancestor) because it tests
/// the granule's actual range directly, without first lifting the interval
/// to a (larger) common ancestor cell.
///
/// Algorithm: binary-search for the first covering cell not entirely before
/// cell_min, then verify that it starts before cell_max ends.
/// Conservative: false positives are possible, false negatives are not.
///
/// Special case: face cells (level 0) have enormous Hilbert ranges that
/// may span regions far from their cell_id sort position. If the granule
/// contains a face cell, we conservatively return true to avoid false
/// negatives. Face cells are only inserted by `insertFaceCellsForRow` for
/// rows with undecodable geometry, so this affects very few granules.
static bool coveringIntersectsRange(
    const S2CellUnion & covering,
    S2CellId cell_min,
    S2CellId cell_max)
{
    /// Face cells (level 0) have Hilbert ranges spanning an entire cube face.
    /// Their cell_id sort position may be far from their range_min, causing the
    /// binary search below to miss valid intersections. Conservatively return
    /// true if any endpoint is a face cell.
    if (cell_min.level() == 0 || cell_max.level() == 0)
        return true;

    auto it = std::lower_bound(
        covering.begin(), covering.end(), cell_min,
        [](S2CellId a, S2CellId b) { return a.range_max() < b.range_min(); });
    return it != covering.end()
        && it->range_min() <= cell_max.range_max();
}

// ---------------------------------------------------------------------------
// tryAnalyzeS2Covering
// ---------------------------------------------------------------------------

bool tryAnalyzeS2Covering(
    const RPNBuilderFunctionTreeNode & func,
    const KeyCondition::ColumnIndices & key_columns,
    int s2_max_covering_cells,
    KeyCondition::RPNElement & out)
{
    using RPNElement = KeyCondition::RPNElement;

    const std::string func_name = func.getFunctionName();

    /// Helper: given the index of the argument that holds the key column and a
    /// pre-built covering, fill `out` and return true.
    auto make_result = [&](size_t key_arg_idx, S2CellUnion s2_covering) -> bool
    {
        const auto col_name = func.getArgumentAt(key_arg_idx).getColumnName();
        auto it = key_columns.find(col_name);
        if (it == key_columns.end())
            return false;

        out.key_columns.push_back(it->second);
        out.s2_covering_data = std::make_shared<RPNElement::S2CoveringData>();
        out.s2_covering_data->covering = std::move(s2_covering);
        out.s2_covering_data->function_name = func_name;
        out.function = RPNElement::FUNCTION_S2_COVERING;
        return true;
    };

    // ------------------------------------------------------------------
    // s2RectContains(lo_const, hi_const, s2_point_column)
    // ------------------------------------------------------------------
    if (func_name == "s2RectContains")
    {
        Field lo_val, hi_val;
        DataTypePtr lo_type, hi_type;

        if (!func.getArgumentAt(0).tryGetConstant(lo_val, lo_type))
            return false;
        if (!func.getArgumentAt(1).tryGetConstant(hi_val, hi_type))
            return false;

        UInt64 lo_id = lo_val.safeGet<UInt64>();
        UInt64 hi_id = hi_val.safeGet<UInt64>();

        S2LatLngRect rect(S2CellId(lo_id).ToLatLng(), S2CellId(hi_id).ToLatLng());
        if (!rect.is_valid())
            return false;

        S2RegionCoverer::Options opts;
        opts.set_max_cells(s2_max_covering_cells);
        return make_result(2, S2RegionCoverer(opts).GetCovering(rect));
    }

    // ------------------------------------------------------------------
    // s2CapContains(center_const, degrees_const, s2_point_column)
    // ------------------------------------------------------------------
    if (func_name == "s2CapContains")
    {
        Field center_val, degrees_val;
        DataTypePtr center_type, degrees_type;

        if (!func.getArgumentAt(0).tryGetConstant(center_val, center_type))
            return false;
        if (!func.getArgumentAt(1).tryGetConstant(degrees_val, degrees_type))
            return false;

        UInt64 center_id = center_val.safeGet<UInt64>();
        Float64 degrees = degrees_val.safeGet<Float64>();

        S2Cap cap(S2CellId(center_id).ToPoint(), S1Angle::Degrees(degrees));
        if (!cap.is_valid())
            return false;

        S2RegionCoverer::Options opts;
        opts.set_max_cells(s2_max_covering_cells);
        return make_result(2, S2RegionCoverer(opts).GetCovering(cap));
    }

    // ------------------------------------------------------------------
    // s2CellsIntersect(s2index1, s2index2) -- either arg may be the key column
    //
    // Soundness: wrapping `const_cell` in a single-element S2CellUnion and
    // testing it via coveringIntersectsRange is correct because
    // [const_cell.range_min(), const_cell.range_max()] is exactly the set of
    // all leaf-cell descendants of `const_cell`.  A granule with interval
    // [cell_min, cell_max] can only contain a cell that intersects `const_cell`
    // if some leaf-cell in that interval falls inside const_cell's range --
    // which is precisely what coveringIntersectsRange tests.
    // Conservative: false positives are possible, false negatives are not.
    // ------------------------------------------------------------------
    if (func_name == "s2CellsIntersect")
    {
        Field cell_val;
        DataTypePtr cell_type;
        size_t key_arg_idx;

        if (func.getArgumentAt(0).tryGetConstant(cell_val, cell_type))
            key_arg_idx = 1;
        else if (func.getArgumentAt(1).tryGetConstant(cell_val, cell_type))
            key_arg_idx = 0;
        else
            return false;

        UInt64 const_id = cell_val.safeGet<UInt64>();
        S2CellId const_cell(const_id);
        if (!const_cell.is_valid())
            return false;

        /// A single cell is already a perfect covering -- no S2RegionCoverer needed.
        return make_result(key_arg_idx, S2CellUnion({const_cell}));
    }

    // ------------------------------------------------------------------
    // __s2CoveringIntersects(cell_id, Array(UInt64))
    //
    // Internal function emitted by ProjectionIndexS2 that passes the full
    // multi-cell covering instead of collapsing it to a single ancestor.
    // The Array(UInt64) constant is converted to an S2CellUnion for use
    // by coveringIntersectsRange, which binary-searches the sorted covering.
    // ------------------------------------------------------------------
    if (func_name == "__s2CoveringIntersects")
    {
        /// arg0 = key column (UInt64), arg1 = constant Array(UInt64)
        Field arr_val;
        DataTypePtr arr_type;
        if (!func.getArgumentAt(1).tryGetConstant(arr_val, arr_type))
            return false;

        const auto & arr = arr_val.safeGet<Array>();
        std::vector<S2CellId> cells;
        cells.reserve(arr.size());
        for (const auto & elem : arr)
        {
            S2CellId cell(elem.safeGet<UInt64>());
            if (!cell.is_valid())
                return false;
            cells.push_back(cell);
        }
        if (cells.empty())
            return false;

        /// S2CellUnion constructor auto-normalizes (sorts + deduplicates).
        return make_result(0, S2CellUnion(std::move(cells)));
    }

    return false;
}

// ---------------------------------------------------------------------------
// evalS2Covering
// ---------------------------------------------------------------------------

BoolMask evalS2Covering(
    const KeyCondition::RPNElement & element,
    const Hyperrectangle & hyperrectangle)
{
    /// Conservative fallback if covering was not built (should not happen).
    if (unlikely(!element.s2_covering_data))
        return BoolMask(true, true);

    const Range & key_range = hyperrectangle[element.key_columns[0]];

    /// Unbounded sides: the granule may contain any S2CellId value --
    /// conservatively report a possible intersection to avoid false negatives.
    if (unlikely(
            key_range.left.isNegativeInfinity() || key_range.left.isPositiveInfinity()
            || key_range.right.isNegativeInfinity() || key_range.right.isPositiveInfinity()))
    {
        return BoolMask(true, true);
    }

    S2CellId cell_min(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), key_range.left));
    S2CellId cell_max(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), key_range.right));

    bool intersects = coveringIntersectsRange(
        element.s2_covering_data->covering, cell_min, cell_max);

    /// can_be_false is always true: the covering is conservative and may produce
    /// false positives, so we can never guarantee that every row in the granule
    /// satisfies the predicate.
    return BoolMask(intersects, true);
}

} // namespace DB

#endif // USE_S2_GEOMETRY
