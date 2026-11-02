#include <Interpreters/AdaptiveAggregation/MultiPassTopK/KeySketch.h>

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Passes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/HashTable/Hash.h>

#include <algorithm>
#include <numeric>

namespace DB::MultiPassTopKImpl
{

namespace
{

constexpr auto hot_column_name = "__top_k_hot";

ALWAYS_INLINE UInt32 keySketchHash(UInt64 value)
{
    return static_cast<UInt32>(intHashCRC32(value));
}

ALWAYS_INLINE size_t keySketchCell(UInt64 value)
{
    return keySketchHash(value) >> (32 - key_sketch_cell_bits);
}

ALWAYS_INLINE size_t keySketchFineCell(UInt64 value)
{
    return keySketchHash(value) >> (32 - key_sketch_fine_cell_bits);
}

/// Summarizes the values of its input, a column of the bounding key, in the sketch of its stream, and merges it into the
/// shared sketch at its end. Outputs nothing.
class TopKKeySketchTransform final : public IAccumulatingTransform
{
public:
    TopKKeySketchTransform(SharedHeader header, TopKKeySketchPtr sketch_, size_t stream_)
        : IAccumulatingTransform(std::move(header), std::make_shared<const Block>())
        , sketch(std::move(sketch_))
        , stream(stream_)
    {
    }

    String getName() const override { return "TopKKeySketchTransform"; }

protected:
    void consume(Chunk chunk) override
    {
        /// Allocated by the stream itself, so that the streams zero their sketches in parallel.
        if (!local)
        {
            local = std::make_unique<TopKKeySketchCells>(key_sketch_cells);
            local_fine = std::make_unique<TopKKeySketchFineRows>();
        }
        /// Forced inline: the compiler otherwise keeps the summary of a run out of line, a call for every run of the
        /// loops of the four types of the key.
        forEachKeyRun(chunk.getColumns().front(), [&](UInt64 value, UInt64 count) ALWAYS_INLINE
        {
            const UInt32 hash = keySketchHash(value);
            auto & cell = (*local)[hash >> (32 - key_sketch_cell_bits)];
            cell.rows += count;
            cell.vote(value, count);
            local_fine->add(hash >> (32 - key_sketch_fine_cell_bits), count);
        });
    }

    Chunk generate() override
    {
        if (local)
            sketch->merge(*local, *local_fine, stream);
        return {};
    }

private:
    const TopKKeySketchPtr sketch;
    const size_t stream;
    std::unique_ptr<TopKKeySketchCells> local;
    std::unique_ptr<TopKKeySketchFineRows> local_fine;
};

/// Whether a value of the bounding key falls into a hot fine cell of the sketch and is none of `excluded`, the values of
/// the first restricted pass, whose groups that pass aggregated. A filter of `PREWHERE` (see `makeHotFineCellsFilter`),
/// so the reader skips the other columns of the granules without such values.
class FunctionTopKHotFineCells final : public IFunction
{
public:
    FunctionTopKHotFineCells(std::shared_ptr<const TopKCellSet> hot_cells_, std::shared_ptr<const std::unordered_set<UInt64>> excluded_)
        : hot_cells(std::move(hot_cells_))
        , excluded(std::move(excluded_))
    {
    }

    String getName() const override { return "__topKHotFineCells"; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt8>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result = ColumnUInt8::create(input_rows_count);
        auto & filter = result->getData();
        withKeyData(arguments.front().column, [&](const auto & data)
        {
            for (size_t i = 0; i < data.size(); ++i)
            {
                const UInt64 value = bit_cast<UInt64>(data[i]);
                filter[i] = hot_cells->test(keySketchFineCell(value)) && !excluded->contains(value);
            }
        });
        return result;
    }

private:
    const std::shared_ptr<const TopKCellSet> hot_cells;
    const std::shared_ptr<const std::unordered_set<UInt64>> excluded;
};

class TopKKeySketchStep final : public ITransformingStep
{
public:
    TopKKeySketchStep(const SharedHeader & input_header, TopKKeySketchPtr sketch_)
        : ITransformingStep(input_header, std::make_shared<const Block>(), filtering_traits)
        , sketch(std::move(sketch_))
    {
    }

    String getName() const override { return "TopKKeySketch"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        size_t stream = 0;
        pipeline.addSimpleTransform([&](const SharedHeader & header)
            { return std::make_shared<TopKKeySketchTransform>(header, sketch, stream++); });
    }

private:
    void updateOutputHeader() override { output_header = std::make_shared<const Block>(); }

    const TopKKeySketchPtr sketch;
};

}

void TopKKeySketch::merge(const TopKKeySketchCells & local, const TopKKeySketchFineRows & local_fine, size_t first)
{
    constexpr size_t cells_per_stripe = key_sketch_cells / key_sketch_stripes;
    constexpr size_t fine_cells_per_stripe = key_sketch_fine_cells / key_sketch_stripes;
    /// The overflowed fine cells, in order, so each stripe merges those of its own.
    std::vector<std::pair<UInt32, UInt64>> overflow(local_fine.overflow.begin(), local_fine.overflow.end());
    std::ranges::sort(overflow);
    for (size_t i = 0; i < key_sketch_stripes; ++i)
    {
        const size_t stripe = (first + i) % key_sketch_stripes;
        std::lock_guard lock(mutexes[stripe]);
        for (size_t cell = stripe * cells_per_stripe; cell < (stripe + 1) * cells_per_stripe; ++cell)
        {
            merged[cell].rows += local[cell].rows;
            merged[cell].vote(local[cell].value, local[cell].votes);
        }
        /// A byte holds fewer rows than overflow, so the bytes add without a branch, which vectorizes.
        const size_t stripe_begin = stripe * fine_cells_per_stripe;
        const UInt8 * __restrict local_rows = local_fine.rows.data() + stripe_begin;
        UInt64 * __restrict merged_rows = fine_rows.data() + stripe_begin;
        for (size_t cell = 0; cell < fine_cells_per_stripe; ++cell)
            merged_rows[cell] += local_rows[cell];
        for (auto it = std::ranges::lower_bound(overflow, std::pair{static_cast<UInt32>(stripe_begin), UInt64{0}});
             it != overflow.end() && it->first < stripe_begin + fine_cells_per_stripe;
             ++it)
            fine_rows[it->first] += it->second;
    }
}

UInt64 TopKKeySketch::rows() const
{
    UInt64 rows = 0;
    for (const auto & cell : merged)
        rows += cell.rows;
    return rows;
}

std::unordered_set<UInt64> TopKKeySketch::candidatesOfHeaviestCells(size_t count) const
{
    std::vector<UInt32> order(key_sketch_cells);
    std::iota(order.begin(), order.end(), 0);
    const size_t num_heaviest = std::min(key_sketch_cells, count);
    std::nth_element(order.begin(), order.begin() + (num_heaviest - 1), order.end(),
        [&](UInt32 lhs, UInt32 rhs) { return merged[lhs].rows > merged[rhs].rows; });
    /// Values repeat: a cell whose streams' votes all cancelled out keeps the value 0 that it started with, which
    /// other cells may hold as well.
    std::unordered_set<UInt64> candidates;
    for (size_t i = 0; i < num_heaviest; ++i)
        if (const auto & cell = merged[order[i]]; cell.rows != 0)
            candidates.insert(cell.value);
    return candidates;
}

UInt64 TopKKeySketch::minRowsOf(const std::unordered_set<UInt64> & values) const
{
    UInt64 rows = 0;
    for (const UInt64 value : values)
        rows += merged[keySketchCell(value)].minRowsOf(value);
    return rows;
}

TopKOtherValueBounds TopKKeySketch::boundsOfOtherValues(
    const std::unordered_set<UInt64> & taken, const std::unordered_map<UInt64, UInt64> & rows_of_taken) const
{
    TopKOtherValueBounds bounds;
    bounds.cells.resize(key_sketch_cells);
    for (size_t cell = 0; cell < key_sketch_cells; ++cell)
        bounds.cells[cell] = merged[cell].rows;
    for (const auto & [value, value_rows] : rows_of_taken)
    {
        bounds.cells[keySketchCell(value)] -= value_rows;
        bounds.fine_rows_of_taken[keySketchFineCell(value)] += value_rows;
        bounds.rows_of_taken += value_rows;
    }
    for (size_t cell = 0; cell < key_sketch_cells; ++cell)
    {
        const auto & sketch_cell = merged[cell];
        const UInt64 by_votes = taken.contains(sketch_cell.value) ? sketch_cell.maxRowsOfOther() : sketch_cell.maxRowsOfAny();
        bounds.cells[cell] = std::min(bounds.cells[cell], by_votes);
    }
    return bounds;
}

std::pair<std::shared_ptr<TopKCellSet>, UInt64> TopKKeySketch::hotFineCells(const TopKOtherValueBounds & bounds, UInt64 min_rows) const
{
    auto hot_cells = std::make_shared<TopKCellSet>(key_sketch_fine_cells);
    UInt64 hot_rows = 0;
    for (size_t cell = 0; cell < key_sketch_cells; ++cell)
    {
        if (bounds.cells[cell] < min_rows)
            continue;
        for (size_t fine = cell * key_sketch_fine_cells_per_cell; fine < (cell + 1) * key_sketch_fine_cells_per_cell; ++fine)
        {
            if (fine_rows[fine] < min_rows)
                continue;
            const auto taken = bounds.fine_rows_of_taken.find(fine);
            const UInt64 other_rows = fine_rows[fine] - (taken == bounds.fine_rows_of_taken.end() ? 0 : taken->second);
            if (other_rows >= min_rows)
            {
                hot_cells->set(fine);
                hot_rows += other_rows;
            }
        }
    }
    return {std::move(hot_cells), hot_rows};
}

TopKKeyFilter makeHotFineCellsFilter(
    const TopKBoundingKey & key, std::shared_ptr<const TopKCellSet> hot_cells, std::shared_ptr<const std::unordered_set<UInt64>> excluded)
{
    ActionsDAG dag;
    const auto & column = dag.addInput(key.column, key.type);
    auto function = std::make_shared<FunctionToFunctionBaseAdaptor>(
        std::make_shared<FunctionTopKHotFineCells>(std::move(hot_cells), std::move(excluded)), DataTypes{key.type}, std::make_shared<DataTypeUInt8>());
    const auto & hot = dag.addFunction(std::move(function), {&column}, hot_column_name);
    dag.getOutputs() = {&column, &hot};
    return {.dag = std::move(dag), .column = hot_column_name};
}

QueryPlan makeKeySketchPlan(const MultiPassTopKTemplate & templ, const TopKKeySketchPtr & sketch)
{
    QueryPlan plan = makeInputPlan(templ, cloneRead(templ, templ.input->context));
    /// The pass reads only the column of the key and the columns of the filters.
    const auto & header = plan.getCurrentHeader();
    ActionsDAG projection(header->getColumnsWithTypeAndName());
    projection.removeUnusedActions(Names{templ.input->bounding_key->name}, /*allow_remove_inputs=*/false);
    plan.addStep(std::make_unique<ExpressionStep>(header, std::move(projection)));
    QueryPlanOptimizations::removeUnusedColumns(*plan.getRootNode(), QueryPlanOptimizations::RemoveUnusedColumnsMode::Global);
    plan.addStep(std::make_unique<TopKKeySketchStep>(plan.getCurrentHeader(), sketch));
    return plan;
}

}
