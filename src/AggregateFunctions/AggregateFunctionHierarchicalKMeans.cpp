#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/FunctionDocumentation.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>

#include <pcg_random.hpp>

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>

/// `hierarchicalKMeans(k [, branching] [, max_iter] [, sample_cap] [, seed])(vec)`
///
/// Parametric aggregate that trains `k` cluster centroids from the aggregated vectors and returns them as
/// `Array(Array(Float32))` (k rows of `dim` floats). Intended to build the coarse quantizer for an IVF vector index:
///
///     INSERT INTO centroids
///     SELECT rowNumberInAllBlocks()::UInt32 AS cid, c
///     FROM (SELECT arrayJoin(hierarchicalKMeans(32768)(vec)) AS c FROM sample);
///
/// The table feeding it may hold anywhere from ~1-2M to billions of vectors: the aggregate keeps a bounded
/// RESERVOIR of `sample_cap` vectors (uniform sample), so memory is O(sample_cap * dim) regardless of input size.
/// A small table (< sample_cap) is kept in full; a billion-row table is uniformly downsampled. k-means centroids
/// depend on the data distribution, not the count, so a large sample yields essentially the same centroids as
/// training on all rows.
///
/// "Hierarchical" refers to the TRAINING algorithm, not the output: centroids are grown by a recursive
/// split (branch `branching` at each level), so per-point work is O(branching * log_branching(k)) instead of the
/// O(k) of flat Lloyd - this is what makes k in the tens of thousands trainable. The OUTPUT is the flat set of
/// `k` leaf centroids (the tree is scaffolding, discarded). Leaves are allocated to children proportional to
/// population, which keeps final cell sizes balanced (anti-skew).

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

namespace
{

using Float = Float32;

/// Flat Lloyd k-means: `n` points of dimension `d` -> `k` row-major centroids (`k * d` floats).
/// k-means++ seeding, argmin via the reformulation ||x - c||^2 = ||x||^2 + ||c||^2 - 2 x.c (||x||^2 dropped),
/// and empty-cluster reseeding.
VectorWithMemoryTracking<Float> kMeansLloyd(const Float * pts, size_t n, size_t d, size_t k, size_t iters, pcg64 & rng)
{
    k = std::min(k, n);
    VectorWithMemoryTracking<Float> centroids(k * d);
    if (k == 0)
        return centroids;

    /// --- k-means++ initialization ---
    VectorWithMemoryTracking<double> best_d2(n, std::numeric_limits<double>::max());
    {
        size_t first = rng() % n;
        std::copy(pts + first * d, pts + (first + 1) * d, centroids.begin());
    }
    for (size_t c = 1; c < k; ++c)
    {
        const Float * prev = &centroids[(c - 1) * d];
        double sum = 0;
        for (size_t i = 0; i < n; ++i)
        {
            const Float * x = pts + i * d;
            double dd = 0;
            for (size_t j = 0; j < d; ++j)
            {
                double t = static_cast<double>(x[j]) - static_cast<double>(prev[j]);
                dd += t * t;
            }
            best_d2[i] = std::min(best_d2[i], dd);
            sum += best_d2[i];
        }
        double r = (static_cast<double>(rng()) / (static_cast<double>(std::numeric_limits<UInt64>::max()) + 1.0)) * sum;
        double acc = 0;
        size_t pick = n - 1;
        for (size_t i = 0; i < n; ++i)
        {
            acc += best_d2[i];
            if (acc >= r)
            {
                pick = i;
                break;
            }
        }
        std::copy(pts + pick * d, pts + (pick + 1) * d, &centroids[c * d]);
    }

    /// --- Lloyd iterations ---
    VectorWithMemoryTracking<size_t> assignment(n, static_cast<size_t>(-1));
    VectorWithMemoryTracking<size_t> counts(k);
    VectorWithMemoryTracking<double> sums(k * d);
    VectorWithMemoryTracking<double> cnorm(k);
    for (size_t iteration = 0; iteration < iters; ++iteration)
    {
        for (size_t c = 0; c < k; ++c)
        {
            const Float * cen = &centroids[c * d];
            double s = 0;
            for (size_t j = 0; j < d; ++j)
                s += static_cast<double>(cen[j]) * static_cast<double>(cen[j]);
            cnorm[c] = s;
        }

        bool changed = false;
        for (size_t i = 0; i < n; ++i)
        {
            const Float * x = pts + i * d;
            size_t best = 0;
            double best_score = std::numeric_limits<double>::max();
            for (size_t c = 0; c < k; ++c)
            {
                const Float * cen = &centroids[c * d];
                double dot = 0;
                for (size_t j = 0; j < d; ++j)
                    dot += static_cast<double>(x[j]) * static_cast<double>(cen[j]);
                double score = cnorm[c] - 2.0 * dot;
                if (score < best_score)
                {
                    best_score = score;
                    best = c;
                }
            }
            if (assignment[i] != best)
            {
                assignment[i] = best;
                changed = true;
            }
        }

        std::fill(sums.begin(), sums.end(), 0.0);
        std::fill(counts.begin(), counts.end(), 0);
        for (size_t i = 0; i < n; ++i)
        {
            size_t c = assignment[i];
            ++counts[c];
            const Float * x = pts + i * d;
            for (size_t j = 0; j < d; ++j)
                sums[c * d + j] += static_cast<double>(x[j]);
        }
        for (size_t c = 0; c < k; ++c)
        {
            if (counts[c] == 0)
            {
                size_t r = rng() % n; /// reseed an empty cluster with a random point
                std::copy(pts + r * d, pts + (r + 1) * d, &centroids[c * d]);
                continue;
            }
            for (size_t j = 0; j < d; ++j)
                centroids[c * d + j] = static_cast<Float>(sums[c * d + j] / static_cast<double>(counts[c]));
        }

        if (!changed && iteration > 0)
            break;
    }
    return centroids;
}

/// Give each of `B` children at least one leaf, total exactly `k`, extra leaves allocated proportional to
/// population (largest-remainder). Requires k >= B (guaranteed: caller uses branching = min(b, k)).
VectorWithMemoryTracking<size_t> apportion(const VectorWithMemoryTracking<size_t> & pop, size_t k)
{
    size_t B = pop.size();
    VectorWithMemoryTracking<size_t> leaves(B, 1);
    if (k <= B)
    {
        for (size_t c = 0; c < B; ++c)
            leaves[c] = (c < k) ? 1 : 0;
        return leaves;
    }

    size_t remaining = k - B;
    size_t total = std::accumulate(pop.begin(), pop.end(), static_cast<size_t>(0));
    VectorWithMemoryTracking<double> frac(B, 0.0);
    size_t handed = 0;
    for (size_t c = 0; c < B; ++c)
    {
        double exact = total ? static_cast<double>(remaining) * static_cast<double>(pop[c]) / static_cast<double>(total)
                             : static_cast<double>(remaining) / static_cast<double>(B);
        size_t add = static_cast<size_t>(std::floor(exact));
        leaves[c] += add;
        frac[c] = exact - static_cast<double>(add);
        handed += add;
    }
    VectorWithMemoryTracking<size_t> order(B);
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return frac[a] > frac[b]; });
    for (size_t i = 0; i < remaining - handed; ++i)
        ++leaves[order[i]];
    return leaves;
}

/// Recursive hierarchical k-means. Appends the resulting `k` leaf centroids (each `d` floats) to `out`.
void trainHierarchical(
    const Float * pts, size_t n, size_t d, size_t k, size_t branching, size_t iters,
    pcg64 & rng, PaddedPODArray<Float> & out)
{
    if (k == 0 || n == 0)
        return;

    if (k >= n) /// fewer points than requested leaves: every point becomes a centroid
    {
        out.insert(pts, pts + n * d);
        return;
    }

    if (k <= branching) /// base case: a single flat k-means with `k` clusters
    {
        auto centroids = kMeansLloyd(pts, n, d, k, iters, rng);
        out.insert(centroids.data(), centroids.data() + centroids.size());
        return;
    }

    size_t br = branching;
    auto node = kMeansLloyd(pts, n, d, br, iters, rng);

    VectorWithMemoryTracking<double> cnorm(br);
    for (size_t c = 0; c < br; ++c)
    {
        const Float * cen = &node[c * d];
        double s = 0;
        for (size_t j = 0; j < d; ++j)
            s += static_cast<double>(cen[j]) * static_cast<double>(cen[j]);
        cnorm[c] = s;
    }

    VectorWithMemoryTracking<VectorWithMemoryTracking<UInt32>> buckets(br);
    for (size_t i = 0; i < n; ++i)
    {
        const Float * x = pts + i * d;
        size_t best = 0;
        double best_score = std::numeric_limits<double>::max();
        for (size_t c = 0; c < br; ++c)
        {
            const Float * cen = &node[c * d];
            double dot = 0;
            for (size_t j = 0; j < d; ++j)
                dot += static_cast<double>(x[j]) * static_cast<double>(cen[j]);
            double score = cnorm[c] - 2.0 * dot;
            if (score < best_score)
            {
                best_score = score;
                best = c;
            }
        }
        buckets[best].push_back(static_cast<UInt32>(i));
    }

    VectorWithMemoryTracking<size_t> pop(br);
    for (size_t c = 0; c < br; ++c)
        pop[c] = buckets[c].size();
    auto leaves = apportion(pop, k);

    PaddedPODArray<Float> child;
    for (size_t c = 0; c < br; ++c)
    {
        if (leaves[c] == 0 || buckets[c].empty())
            continue;
        if (leaves[c] == 1) /// keep the node centroid itself as the single leaf
        {
            out.insert(node.data() + c * d, node.data() + (c + 1) * d);
            continue;
        }
        child.resize(buckets[c].size() * d);
        for (size_t j = 0; j < buckets[c].size(); ++j)
            memcpy(&child[j * d], pts + static_cast<size_t>(buckets[c][j]) * d, d * sizeof(Float));
        trainHierarchical(child.data(), buckets[c].size(), d, leaves[c], branching, iters, rng, out);
    }
}

/// Aggregate state: a bounded reservoir of training vectors (uniform sample of the input stream).
struct HierarchicalKMeansData
{
    PaddedPODArray<Float> samples; /// flat, (samples.size() / dim) vectors
    UInt64 seen = 0;
    UInt32 dim = 0;
    pcg64 rng; /// seeded in create()

    void addVector(const Float * v, UInt32 d, UInt64 cap)
    {
        if (dim == 0)
            dim = d;
        if (d != dim)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "hierarchicalKMeans: got a vector of size {} but expected {}", d, dim);

        ++seen;
        UInt64 have = samples.size() / dim;
        if (have < cap)
        {
            samples.insert(v, v + d);
        }
        else
        {
            UInt64 j = rng() % seen; /// Algorithm R reservoir sampling
            if (j < cap)
                memcpy(&samples[j * dim], v, d * sizeof(Float));
        }
    }

    void merge(const HierarchicalKMeansData & other, UInt64 cap)
    {
        if (other.dim == 0)
            return;
        if (dim == 0)
        {
            dim = other.dim;
            rng = other.rng;
        }
        if (other.dim != dim)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "hierarchicalKMeans: dim mismatch on merge");

        UInt64 d = dim;
        UInt64 have_a = samples.size() / d;
        UInt64 have_b = other.samples.size() / d;

        if (have_a + have_b <= cap)
        {
            samples.insert(other.samples.begin(), other.samples.end());
            seen += other.seen;
            return;
        }

        /// Weighted subsample of the union: keep counts proportional to `seen` so the result stays ~uniform.
        UInt64 total_seen = seen + other.seen;
        UInt64 take_a = std::min<UInt64>(have_a, total_seen ? static_cast<UInt64>(static_cast<double>(cap) * static_cast<double>(seen) / static_cast<double>(total_seen)) : cap / 2);
        UInt64 take_b = std::min<UInt64>(have_b, cap - take_a);
        take_a = std::min<UInt64>(have_a, cap - take_b);

        for (UInt64 i = 0; i < take_a; ++i) /// partial Fisher-Yates: keep take_a random rows of `this`
        {
            UInt64 j = i + rng() % (have_a - i);
            for (UInt64 t = 0; t < d; ++t)
                std::swap(samples[i * d + t], samples[j * d + t]);
        }
        samples.resize(take_a * d);

        VectorWithMemoryTracking<UInt64> idx(have_b);
        std::iota(idx.begin(), idx.end(), 0);
        for (UInt64 i = 0; i < take_b; ++i) /// append take_b random rows of `other`
        {
            UInt64 j = i + rng() % (have_b - i);
            std::swap(idx[i], idx[j]);
            samples.insert(&other.samples[idx[i] * d], &other.samples[(idx[i] + 1) * d]);
        }
        seen += other.seen;
    }
};

class AggregateFunctionHierarchicalKMeans final
    : public IAggregateFunctionDataHelper<HierarchicalKMeansData, AggregateFunctionHierarchicalKMeans>
{
    size_t k;
    size_t branching;
    size_t max_iter;
    UInt64 sample_cap;
    UInt64 seed;

public:
    AggregateFunctionHierarchicalKMeans(const DataTypes & args, const Array & params)
        : IAggregateFunctionDataHelper<HierarchicalKMeansData, AggregateFunctionHierarchicalKMeans>(args, params, createResultType())
    {
        if (params.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function hierarchicalKMeans requires at least the parameter k");

        k          = params[0].safeGet<UInt64>();
        branching  = params.size() > 1 ? params[1].safeGet<UInt64>() : 16;
        max_iter   = params.size() > 2 ? params[2].safeGet<UInt64>() : 20;
        sample_cap = params.size() > 3 ? params[3].safeGet<UInt64>() : 1'000'000;
        seed       = params.size() > 4 ? params[4].safeGet<UInt64>() : 0;

        if (k == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: k must be greater than 0");
        branching = std::max<size_t>(branching, 2);
        max_iter = std::max<size_t>(max_iter, 1);
        if (sample_cap == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "hierarchicalKMeans: sample_cap must be greater than 0");

        const auto * array_type = typeid_cast<const DataTypeArray *>(args[0].get());
        if (!array_type || !isFloat(array_type->getNestedType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function hierarchicalKMeans requires an Array(Float32) argument "
                "(CAST an Array(BFloat16) column to Array(Float32) first)");
    }

    String getName() const override { return "hierarchicalKMeans"; }

    bool allocatesMemoryInArena() const override { return false; }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()));
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) HierarchicalKMeansData();
        data(place).rng.seed(seed);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & array = assert_cast<const ColumnArray &>(*columns[0]);
        const auto & nested = assert_cast<const ColumnFloat32 &>(array.getData()).getData();
        const auto & offsets = array.getOffsets();
        size_t start = row_num ? offsets[row_num - 1] : 0;
        size_t length = offsets[row_num] - start;
        data(place).addVector(&nested[start], static_cast<UInt32>(length), sample_cap);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs), sample_cap);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        const auto & d = data(place);
        writeBinary(d.dim, buf);
        writeBinary(d.seen, buf);
        writeVarUInt(d.samples.size(), buf);
        buf.write(reinterpret_cast<const char *>(d.samples.data()), d.samples.size() * sizeof(Float));
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        auto & d = data(place);
        readBinary(d.dim, buf);
        readBinary(d.seen, buf);
        size_t n = 0;
        readVarUInt(n, buf);
        d.samples.resize(n);
        buf.readStrict(reinterpret_cast<char *>(d.samples.data()), n * sizeof(Float));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & d = data(place);
        auto & outer = assert_cast<ColumnArray &>(to);
        auto & inner = assert_cast<ColumnArray &>(outer.getData());
        auto & values = assert_cast<ColumnFloat32 &>(inner.getData()).getData();

        if (d.dim == 0) /// empty input -> empty array of centroids
        {
            outer.getOffsets().push_back(inner.getOffsets().size());
            return;
        }

        pcg64 train_rng(seed); /// deterministic training regardless of ingest/merge order
        PaddedPODArray<Float> centroids;
        trainHierarchical(d.samples.data(), d.samples.size() / d.dim, d.dim, k, branching, max_iter, train_rng, centroids);

        size_t produced = centroids.size() / d.dim;
        for (size_t c = 0; c < produced; ++c)
        {
            values.insert(centroids.data() + c * d.dim, centroids.data() + (c + 1) * d.dim);
            inner.getOffsets().push_back(values.size());
        }
        outer.getOffsets().push_back(inner.getOffsets().size());
    }
};

AggregateFunctionPtr createAggregateFunctionHierarchicalKMeans(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires exactly one argument (the vector column)", name);
    return std::make_shared<AggregateFunctionHierarchicalKMeans>(argument_types, parameters);
}

}

void registerAggregateFunctionHierarchicalKMeans(AggregateFunctionFactory & factory);
void registerAggregateFunctionHierarchicalKMeans(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description =
        "Trains k cluster centroids from the aggregated vectors using hierarchical k-means and returns them as Array(Array(Float32)).";
    FunctionDocumentation::Syntax syntax = "hierarchicalKMeans(k[, branching[, max_iter[, sample_cap[, seed]]]])(vec)";
    FunctionDocumentation::Arguments arguments = {
        {"vec", "Input vector to cluster.", {"Array(Float32)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"An array of k centroids.", {"Array(Array(Float32))"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT length(hierarchicalKMeans(256)(vec)) FROM sample", ""}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::MachineLearning;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    AggregateFunctionProperties properties = { .is_order_dependent = false };
    factory.registerFunction("hierarchicalKMeans", {createAggregateFunctionHierarchicalKMeans, documentation, properties});
}

}
