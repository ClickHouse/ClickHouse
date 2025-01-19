#include <gtest/gtest.h>

#include <pcg_random.hpp>
#include <random>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>

#include <Columns/ColumnsNumber.h>
#include <Common/getRandomASCIIString.h>
#include <Common/randomSeed.h>

#include <DataTypes/DataTypesNumber.h>

#include <Interpreters/TableJoin.h>

#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/MergeJoinTransform.h>

#include <Processors/Formats/Impl/PrettyCompactBlockOutputFormat.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>


#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

namespace
{

QueryPipeline buildJoinPipeline(
    std::shared_ptr<ISource> left_source,
    std::shared_ptr<ISource> right_source,
    size_t key_length = 1,
    JoinKind kind = JoinKind::Inner,
    JoinStrictness strictness = JoinStrictness::All,
    ASOFJoinInequality asof_inequality = ASOFJoinInequality::None)
{
    Blocks inputs;
    inputs.emplace_back(left_source->getPort().getHeader());
    inputs.emplace_back(right_source->getPort().getHeader());

    Block out_header;
    for (const auto & input : inputs)
    {
        for (ColumnWithTypeAndName column : input)
        {
            if (&input == &inputs.front())
                column.name = "t1." + column.name;
            else
                column.name = "t2." + column.name;
            out_header.insert(column);
        }
    }

    TableJoin::JoinOnClause on_clause;
    for (size_t i = 0; i < key_length; ++i)
    {
        on_clause.key_names_left.emplace_back(inputs[0].getByPosition(i).name);
        on_clause.key_names_right.emplace_back(inputs[1].getByPosition(i).name);
    }

    auto joining = std::make_shared<MergeJoinTransform>(
        kind,
        strictness,
        on_clause,
        inputs, out_header, /* max_block_size = */ 0);

    if (asof_inequality != ASOFJoinInequality::None)
        joining->setAsofInequality(asof_inequality);

    chassert(joining->getInputs().size() == 2);

    connect(left_source->getPort(), joining->getInputs().front());
    connect(right_source->getPort(), joining->getInputs().back());

    auto * output_port = &joining->getOutputPort();

    auto processors = std::make_shared<Processors>();
    processors->emplace_back(std::move(left_source));
    processors->emplace_back(std::move(right_source));
    processors->emplace_back(std::move(joining));

    QueryPipeline pipeline(QueryPlanResourceHolder{}, processors, output_port);
    return pipeline;
}


std::shared_ptr<ISource> oneColumnSource(const std::vector<std::vector<UInt64>> & values)
{
    Block header = {
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "idx"),
    };

    UInt64 idx = 0;
    Chunks chunks;
    for (const auto & chunk_values : values)
    {
        auto key_column = ColumnUInt64::create();
        auto idx_column = ColumnUInt64::create();

        for (auto n : chunk_values)
        {
            key_column->insertValue(n);
            idx_column->insertValue(idx);
            ++idx;
        }
        chunks.emplace_back(Chunk(Columns{std::move(key_column), std::move(idx_column)}, chunk_values.size()));
    }
    return std::make_shared<SourceFromChunks>(header, std::move(chunks));
}

class SourceChunksBuilder
{
public:

    explicit SourceChunksBuilder(const Block & header_)
        : header(header_)
    {
        current_chunk = header.cloneEmptyColumns();
        chassert(!current_chunk.empty());
    }

    void setBreakProbability(pcg64 & rng_)
    {
        /// random probability with possibility to have exact 0.0 and 1.0 values
        break_prob = std::uniform_int_distribution<size_t>(0, 5)(rng_) / static_cast<double>(5);
        rng = &rng_;
    }

    void addRow(const std::vector<Field> & row)
    {
        chassert(row.size() == current_chunk.size());
        for (size_t i = 0; i < current_chunk.size(); ++i)
            current_chunk[i]->insert(row[i]);

        if (rng && std::uniform_real_distribution<>(0.0, 1.0)(*rng) < break_prob)
            addChunk();
    }

    void addChunk()
    {
        if (current_chunk.front()->empty())
            return;

        size_t rows = current_chunk.front()->size();
        chunks.emplace_back(std::move(current_chunk), rows);
        current_chunk = header.cloneEmptyColumns();
    }

    std::shared_ptr<ISource> getSource()
    {
        addChunk();

        /// copy chunk to allow reusing same builder
        Chunks chunks_copy;
        chunks_copy.reserve(chunks.size());
        for (const auto & chunk : chunks)
            chunks_copy.emplace_back(chunk.clone());
        return std::make_shared<SourceFromChunks>(header, std::move(chunks_copy));
    }

private:
    Block header;
    Chunks chunks;
    MutableColumns current_chunk;

    pcg64 * rng = nullptr;
    double break_prob = 0.0;
};


std::vector<std::vector<Field>> getValuesFromBlock(const Block & block, const Names & names)
{
    std::vector<std::vector<Field>> result;
    for (size_t i = 0; i < block.rows(); ++i)
    {
        auto & row = result.emplace_back();
        for (const auto & name : names)
            block.getByName(name).column->get(i, row.emplace_back());
    }
    return result;
}


Block executePipeline(QueryPipeline && pipeline)
{
    PullingPipelineExecutor executor(pipeline);

    Blocks result_blocks;
    while (true)
    {
        Block block;
        bool is_ok = executor.pull(block);
        if (!is_ok)
            break;
        result_blocks.emplace_back(std::move(block));
    }

    return concatenateBlocks(result_blocks);
}

template <typename T>
void assertColumnVectorEq(const typename ColumnVector<T>::Container & expected, const Block & block, const std::string & name)
{
    if (expected.empty())
    {
        ASSERT_TRUE(block.columns() == 0);
        return;
    }

    const auto * actual = typeid_cast<const ColumnVector<T> *>(block.getByName(name).column.get());
    ASSERT_TRUE(actual) << "unexpected column type: " << block.getByName(name).column->dumpStructure() << "expected: " << typeid(ColumnVector<T>).name();

    auto get_first_diff = [&]() -> String
    {
        const auto & actual_data = actual->getData();
        size_t num_rows = std::min(expected.size(), actual_data.size());
        for (size_t i = 0; i < num_rows; ++i)
        {
            if (expected[i] != actual_data[i])
                return fmt::format(", expected: {}, actual: {} at row {}", expected[i], actual_data[i], i);
        }
        return "";
    };

    EXPECT_EQ(actual->getData().size(), expected.size());
    ASSERT_EQ(actual->getData(), expected) << "column name: " << name << get_first_diff();
}

template <typename T>
void assertColumnEq(const IColumn & expected, const Block & block, const std::string & name)
{
    if (expected.empty())
    {
        ASSERT_TRUE(block.columns() == 0);
        return;
    }

    const ColumnPtr & actual = block.getByName(name).column;
    ASSERT_TRUE(checkColumn<T>(*actual));
    ASSERT_TRUE(checkColumn<T>(expected));
    EXPECT_EQ(actual->size(), expected.size());

    auto dump_val = [](const IColumn & col, size_t i) -> String
    {
        Field value;
        col.get(i, value);
        return value.dump();
    };

    size_t num_rows = std::min(actual->size(), expected.size());
    for (size_t i = 0; i < num_rows; ++i)
        ASSERT_EQ(actual->compareAt(i, i, expected, 1), 0) << dump_val(*actual, i) << " != " << dump_val(expected, i) << " at row " << i;
}

template <typename T>
T getRandomFrom(pcg64 & rng, const std::initializer_list<T> & opts)
{
    std::vector<T> options(opts.begin(), opts.end());
    size_t idx = std::uniform_int_distribution<size_t>(0, options.size() - 1)(rng);
    return options[idx];
}

void generateNextKey(pcg64 & rng, UInt64 & k1, String & k2)
{
    size_t str_len = std::uniform_int_distribution<>(1, 10)(rng);
    String new_k2 = getRandomASCIIString(str_len, rng);
    if (new_k2.compare(k2) <= 0)
        ++k1;
    k2 = new_k2;
}

bool isStrict(ASOFJoinInequality inequality)
{
    return inequality == ASOFJoinInequality::Less || inequality == ASOFJoinInequality::Greater;
}

}

class FullSortingJoinTest : public ::testing::Test
{
public:
    FullSortingJoinTest() = default;

    void SetUp() override
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);
        if (const char * test_log_level = std::getenv("TEST_LOG_LEVEL")) // NOLINT(concurrency-mt-unsafe)
            Poco::Logger::root().setLevel(test_log_level);
        else
            Poco::Logger::root().setLevel("none");


        UInt64 seed = randomSeed();
        if (const char * random_seed = std::getenv("TEST_RANDOM_SEED")) // NOLINT(concurrency-mt-unsafe)
            seed = std::stoull(random_seed);
        std::cout << "TEST_RANDOM_SEED=" << seed << std::endl;
        rng = pcg64(seed);
    }

    void TearDown() override
    {
    }

    pcg64 rng;
};

TEST_F(FullSortingJoinTest, AllAnyOneKey)
try
{
    {
        SCOPED_TRACE("Inner All");
        Block result = executePipeline(buildJoinPipeline(
            oneColumnSource({ {1, 2, 3, 4, 5} }),
            oneColumnSource({ {1}, {2}, {3}, {4}, {5} }),
            1, JoinKind::Inner, JoinStrictness::All));

        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({0, 1, 2, 3, 4}), result, "t1.idx");
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({0, 1, 2, 3, 4}), result, "t2.idx");
    }
    {
        SCOPED_TRACE("Inner Any");
        Block result = executePipeline(buildJoinPipeline(
            oneColumnSource({ {1, 2, 3, 4, 5} }),
            oneColumnSource({ {1}, {2}, {3}, {4}, {5} }),
            1, JoinKind::Inner, JoinStrictness::Any));
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({0, 1, 2, 3, 4}), result, "t1.idx");
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({0, 1, 2, 3, 4}), result, "t2.idx");
    }
    {
        SCOPED_TRACE("Inner All");
        Block result = executePipeline(buildJoinPipeline(
            oneColumnSource({ {2, 2, 2}, {2, 3}, {3, 5} }),
            oneColumnSource({ {1, 1, 1}, {2, 2}, {3, 4} }),
            1, JoinKind::Inner, JoinStrictness::All));
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({0, 1, 2, 0, 1, 2, 3, 3, 4, 5}), result, "t1.idx");
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({3, 3, 3, 4, 4, 4, 3, 4, 5, 5}), result, "t2.idx");
    }
    {
        SCOPED_TRACE("Inner Any");
        Block result = executePipeline(buildJoinPipeline(
            oneColumnSource({ {2, 2, 2}, {2, 3}, {3, 5} }),
            oneColumnSource({ {1, 1, 1}, {2, 2}, {3, 4} }),
            1, JoinKind::Inner, JoinStrictness::Any));
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({0, 4}), result, "t1.idx");
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({3, 5}), result, "t2.idx");
    }
    {
        SCOPED_TRACE("Inner Any");
        Block result = executePipeline(buildJoinPipeline(
            oneColumnSource({ {2, 2, 2, 2}, {3}, {3, 5} }),
            oneColumnSource({ {1, 1, 1, 2}, {2}, {3, 4} }),
            1, JoinKind::Inner, JoinStrictness::Any));
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({0, 4}), result, "t1.idx");
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({3, 5}), result, "t2.idx");
    }
    {

        SCOPED_TRACE("Left Any");
        Block result = executePipeline(buildJoinPipeline(
            oneColumnSource({ {2, 2, 2}, {2, 3}, {3, 5} }),
            oneColumnSource({ {1, 1, 1}, {2, 2}, {3, 4} }),
            1, JoinKind::Left, JoinStrictness::Any));
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({0, 1, 2, 3, 4, 5, 6}), result, "t1.idx");
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({3, 3, 3, 3, 5, 5, 0}), result, "t2.idx");
    }
    {
        SCOPED_TRACE("Left Any");
        Block result = executePipeline(buildJoinPipeline(
            oneColumnSource({ {2, 2, 2, 2}, {3}, {3, 5} }),
            oneColumnSource({ {1, 1, 1, 2}, {2}, {3, 4} }),
            1, JoinKind::Left, JoinStrictness::Any));
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({0, 1, 2, 3, 4, 5, 6}), result, "t1.idx");
        assertColumnVectorEq<UInt64>(ColumnUInt64::Container({3, 3, 3, 3, 5, 5, 0}), result, "t2.idx");
    }
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}


TEST_F(FullSortingJoinTest, AnySimple)
try
{
    JoinKind kind = getRandomFrom(rng, {JoinKind::Inner, JoinKind::Left, JoinKind::Right});

    SourceChunksBuilder left_source({
        {std::make_shared<DataTypeUInt64>(), "k1"},
        {std::make_shared<DataTypeString>(), "k2"},
        {std::make_shared<DataTypeString>(), "attr"},
    });

    SourceChunksBuilder right_source({
        {std::make_shared<DataTypeUInt64>(), "k1"},
        {std::make_shared<DataTypeString>(), "k2"},
        {std::make_shared<DataTypeString>(), "attr"},
    });

    left_source.setBreakProbability(rng);
    right_source.setBreakProbability(rng);

    size_t num_keys = std::uniform_int_distribution<>(100, 1000)(rng);

    auto expected_left = ColumnString::create();
    auto expected_right = ColumnString::create();

    UInt64 k1 = 1;
    String k2;

    auto get_attr = [&](const String & side, size_t idx) -> String
    {
        return toString(k1) + "_" + k2 + "_" + side + "_" + toString(idx);
    };

    for (size_t i = 0; i < num_keys; ++i)
    {
        generateNextKey(rng, k1, k2);

        /// Key is present in left, right or both tables. Both tables is more probable.
        size_t key_presence = std::uniform_int_distribution<>(0, 10)(rng);

        size_t num_rows_left = key_presence == 0 ? 0 : std::uniform_int_distribution<>(1, 10)(rng);
        for (size_t j = 0; j < num_rows_left; ++j)
            left_source.addRow({k1, k2, get_attr("left", j)});

        size_t num_rows_right = key_presence == 1 ? 0 : std::uniform_int_distribution<>(1, 10)(rng);
        for (size_t j = 0; j < num_rows_right; ++j)
            right_source.addRow({k1, k2, get_attr("right", j)});

        String left_attr = num_rows_left ? get_attr("left", 0) : "";
        String right_attr = num_rows_right ? get_attr("right", 0) : "";

        if (kind == JoinKind::Inner && num_rows_left && num_rows_right)
        {
            expected_left->insert(left_attr);
            expected_right->insert(right_attr);
        }
        else if (kind == JoinKind::Left)
        {
            for (size_t j = 0; j < num_rows_left; ++j)
            {
                expected_left->insert(get_attr("left", j));
                expected_right->insert(right_attr);
            }
        }
        else if (kind == JoinKind::Right)
        {
            for (size_t j = 0; j < num_rows_right; ++j)
            {
                expected_left->insert(left_attr);
                expected_right->insert(get_attr("right", j));
            }
        }
    }

    Block result_block = executePipeline(buildJoinPipeline(
        left_source.getSource(), right_source.getSource(), /* key_length = */ 2,
        kind, JoinStrictness::Any));
    assertColumnEq<ColumnString>(*expected_left, result_block, "t1.attr");
    assertColumnEq<ColumnString>(*expected_right, result_block, "t2.attr");
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}

TEST_F(FullSortingJoinTest, AsofSimple)
try
{
    SourceChunksBuilder left_source({
        {std::make_shared<DataTypeString>(), "key"},
        {std::make_shared<DataTypeUInt64>(), "t"},
    });
    left_source.addRow({"AMZN", 3});
    left_source.addRow({"AMZN", 4});
    left_source.addRow({"AMZN", 6});
    left_source.addRow({"SBUX", 10});

    SourceChunksBuilder right_source({
        {std::make_shared<DataTypeString>(), "key"},
        {std::make_shared<DataTypeUInt64>(), "t"},
        {std::make_shared<DataTypeUInt64>(), "value"},
    });
    right_source.addRow({"AAPL", 1, 97});
    right_source.addChunk();
    right_source.addRow({"AAPL", 2, 98});
    right_source.addRow({"AAPL", 3, 99});
    right_source.addRow({"AMZN", 1, 100});
    right_source.addRow({"AMZN", 2, 110});
    right_source.addChunk();
    right_source.addRow({"AMZN", 2, 110});
    right_source.addChunk();
    right_source.addRow({"AMZN", 4, 130});
    right_source.addRow({"AMZN", 5, 140});
    right_source.addRow({"SBUX", 8, 180});
    right_source.addChunk();
    right_source.addRow({"SBUX", 9, 190});

    {
        Block result_block = executePipeline(buildJoinPipeline(
            left_source.getSource(), right_source.getSource(), /* key_length = */ 2,
            JoinKind::Inner, JoinStrictness::Asof, ASOFJoinInequality::LessOrEquals));
        auto values = getValuesFromBlock(result_block, {"t1.key", "t1.t", "t2.t", "t2.value"});

        ASSERT_EQ(values, (std::vector<std::vector<Field>>{
            {"AMZN", 3u, 4u, 130u},
            {"AMZN", 4u, 4u, 130u},
        }));
    }

    {
        Block result_block = executePipeline(buildJoinPipeline(
            left_source.getSource(), right_source.getSource(), /* key_length = */ 2,
            JoinKind::Inner, JoinStrictness::Asof, ASOFJoinInequality::GreaterOrEquals));
        auto values = getValuesFromBlock(result_block, {"t1.key", "t1.t", "t2.t", "t2.value"});

        ASSERT_EQ(values, (std::vector<std::vector<Field>>{
            {"AMZN", 3u, 2u, 110u},
            {"AMZN", 4u, 4u, 130u},
            {"AMZN", 6u, 5u, 140u},
            {"SBUX", 10u, 9u, 190u},
        }));
    }
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}


TEST_F(FullSortingJoinTest, AsofOnlyColumn)
try
{
    auto left_source = oneColumnSource({ {3}, {3, 3, 3}, {3, 5, 5, 6}, {9, 9}, {10, 20} });

    SourceChunksBuilder right_source_builder({
        {std::make_shared<DataTypeUInt64>(), "t"},
        {std::make_shared<DataTypeUInt64>(), "value"},
    });

    right_source_builder.setBreakProbability(rng);

    for (const auto & row : std::vector<std::vector<Field>>{ {1, 101}, {2, 102}, {4, 104}, {5, 105}, {11, 111}, {15, 115} })
        right_source_builder.addRow(row);

    auto right_source = right_source_builder.getSource();

    auto pipeline = buildJoinPipeline(
        left_source, right_source, /* key_length = */ 1,
        JoinKind::Inner, JoinStrictness::Asof, ASOFJoinInequality::LessOrEquals);

    Block result_block = executePipeline(std::move(pipeline));

    ASSERT_EQ(
        assert_cast<const ColumnUInt64 *>(result_block.getByName("t1.key").column.get())->getData(),
        (ColumnUInt64::Container{3, 3, 3, 3, 3, 5, 5, 6, 9, 9, 10})
    );

    ASSERT_EQ(
        assert_cast<const ColumnUInt64 *>(result_block.getByName("t2.t").column.get())->getData(),
        (ColumnUInt64::Container{4, 4, 4, 4, 4, 5, 5, 11, 11, 11, 11})
    );

    ASSERT_EQ(
        assert_cast<const ColumnUInt64 *>(result_block.getByName("t2.value").column.get())->getData(),
        (ColumnUInt64::Container{104, 104, 104, 104, 104, 105, 105, 111, 111, 111, 111})
    );
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}

TEST_F(FullSortingJoinTest, AsofLessGeneratedTestData)
try
{
    /// Generate data random and build expected result at the same time.

    /// Test specific combinations of join kind and inequality per each run
    auto join_kind = getRandomFrom(rng, { JoinKind::Inner, JoinKind::Left });
    auto asof_inequality = getRandomFrom(rng, { ASOFJoinInequality::Less, ASOFJoinInequality::LessOrEquals });

    SCOPED_TRACE(fmt::format("{} {}", join_kind, asof_inequality));

    /// Key is complex, `k1, k2` for equality and `t` for asof
    SourceChunksBuilder left_source_builder({
        {std::make_shared<DataTypeUInt64>(), "k1"},
        {std::make_shared<DataTypeString>(), "k2"},
        {std::make_shared<DataTypeUInt64>(), "t"},
        {std::make_shared<DataTypeInt64>(), "attr"},
    });

    SourceChunksBuilder right_source_builder({
        {std::make_shared<DataTypeUInt64>(), "k1"},
        {std::make_shared<DataTypeString>(), "k2"},
        {std::make_shared<DataTypeUInt64>(), "t"},
        {std::make_shared<DataTypeInt64>(), "attr"},
    });

    /// How small generated block should be
    left_source_builder.setBreakProbability(rng);
    right_source_builder.setBreakProbability(rng);

    /// We are going to generate sorted data and remember expected result
    ColumnInt64::Container expected;

    UInt64 k1 = 1;
    String k2;
    auto key_num_total = std::uniform_int_distribution<>(1, 1000)(rng);
    for (size_t key_num = 0; key_num < key_num_total; ++key_num)
    {
        /// Generate new key greater than previous
        generateNextKey(rng, k1, k2);

        Int64 left_t = 0;
        /// Generate several rows for the key
        size_t num_left_rows = std::uniform_int_distribution<>(1, 100)(rng);
        for (size_t i = 0; i < num_left_rows; ++i)
        {
            /// t is strictly greater than previous
            left_t += std::uniform_int_distribution<>(1, 10)(rng);

            auto left_arrtibute_value = 10 * left_t;
            left_source_builder.addRow({k1, k2, left_t, left_arrtibute_value});
            expected.push_back(left_arrtibute_value);

            auto num_matches = 1 + std::poisson_distribution<>(4)(rng);
            /// Generate several matches in the right table
            auto right_t = left_t;
            for (size_t j = 0; j < num_matches; ++j)
            {
                int min_step = isStrict(asof_inequality) ? 1 : 0;
                right_t += std::uniform_int_distribution<>(min_step, 3)(rng);

                /// First row should match
                bool is_match = j == 0;
                right_source_builder.addRow({k1, k2, right_t, is_match ? 10 * left_arrtibute_value : -1});
            }
            /// Next left_t should be greater than right_t not to match with previous rows
            left_t = right_t;
        }

        /// generate some rows with greater left_t to check that they are not matched
        num_left_rows = std::bernoulli_distribution(0.5)(rng) ? std::uniform_int_distribution<>(1, 100)(rng) : 0;
        for (size_t i = 0; i < num_left_rows; ++i)
        {
            left_t += std::uniform_int_distribution<>(1, 10)(rng);
            left_source_builder.addRow({k1, k2, left_t, -10 * left_t});

            if (join_kind == JoinKind::Left)
                expected.push_back(-10 * left_t);
        }
    }

    Block result_block = executePipeline(buildJoinPipeline(
        left_source_builder.getSource(), right_source_builder.getSource(),
        /* key_length = */ 3,
        join_kind, JoinStrictness::Asof, asof_inequality));

    assertColumnVectorEq<Int64>(expected, result_block, "t1.attr");

    for (auto & e : expected)
        /// Non matched rows from left table have negative attr
        /// Value if attribute in right table is 10 times greater than in left table
        e = e < 0 ? 0 : 10 * e;

    assertColumnVectorEq<Int64>(expected, result_block, "t2.attr");
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}

TEST_F(FullSortingJoinTest, AsofGreaterGeneratedTestData)
try
{
    /// Generate data random and build expected result at the same time.

    /// Test specific combinations of join kind and inequality per each run
    auto join_kind = getRandomFrom(rng, { JoinKind::Inner, JoinKind::Left });
    auto asof_inequality = getRandomFrom(rng, { ASOFJoinInequality::Greater, ASOFJoinInequality::GreaterOrEquals });

    SCOPED_TRACE(fmt::format("{} {}", join_kind, asof_inequality));

    SourceChunksBuilder left_source_builder({
        {std::make_shared<DataTypeUInt64>(), "k1"},
        {std::make_shared<DataTypeString>(), "k2"},
        {std::make_shared<DataTypeUInt64>(), "t"},
        {std::make_shared<DataTypeInt64>(), "attr"},
    });

    SourceChunksBuilder right_source_builder({
        {std::make_shared<DataTypeUInt64>(), "k1"},
        {std::make_shared<DataTypeString>(), "k2"},
        {std::make_shared<DataTypeUInt64>(), "t"},
        {std::make_shared<DataTypeInt64>(), "attr"},
    });

    left_source_builder.setBreakProbability(rng);
    right_source_builder.setBreakProbability(rng);

    ColumnInt64::Container expected;

    UInt64 k1 = 1;
    String k2;
    UInt64 left_t = 0;

    auto key_num_total = std::uniform_int_distribution<>(1, 1000)(rng);
    for (size_t key_num = 0; key_num < key_num_total; ++key_num)
    {
        /// Generate new key greater than previous
        generateNextKey(rng, k1, k2);

        /// Generate some rows with smaller left_t to check that they are not matched
        size_t num_left_rows = std::bernoulli_distribution(0.5)(rng) ? std::uniform_int_distribution<>(1, 100)(rng) : 0;
        for (size_t i = 0; i < num_left_rows; ++i)
        {
            left_t += std::uniform_int_distribution<>(1, 10)(rng);
            left_source_builder.addRow({k1, k2, left_t, -10 * left_t});

            if (join_kind == JoinKind::Left)
                expected.push_back(-10 * left_t);
        }

        if (std::bernoulli_distribution(0.1)(rng))
            continue;

        size_t num_right_matches = std::uniform_int_distribution<>(1, 10)(rng);
        auto right_t = left_t + std::uniform_int_distribution<>(isStrict(asof_inequality) ? 0 : 1, 10)(rng);
        auto attribute_value = 10 * right_t;
        for (size_t j = 0; j < num_right_matches; ++j)
        {
            right_t += std::uniform_int_distribution<>(0, 3)(rng);
            bool is_match = j == num_right_matches - 1;
            right_source_builder.addRow({k1, k2, right_t, is_match ? 10 * attribute_value : -1});
        }

        /// Next left_t should be greater than (or equals) right_t to match with previous rows
        left_t = right_t + std::uniform_int_distribution<>(isStrict(asof_inequality) ? 1 : 0, 100)(rng);
        size_t num_left_matches = std::uniform_int_distribution<>(1, 100)(rng);
        for (size_t j = 0; j < num_left_matches; ++j)
        {
            left_t += std::uniform_int_distribution<>(0, 3)(rng);
            left_source_builder.addRow({k1, k2, left_t, attribute_value});
            expected.push_back(attribute_value);
        }
    }

    Block result_block = executePipeline(buildJoinPipeline(
        left_source_builder.getSource(), right_source_builder.getSource(),
        /* key_length = */ 3,
        join_kind, JoinStrictness::Asof, asof_inequality));

    assertColumnVectorEq<Int64>(expected, result_block, "t1.attr");

    for (auto & e : expected)
        /// Non matched rows from left table have negative attr
        /// Value if attribute in right table is 10 times greater than in left table
        e = e < 0 ? 0 : 10 * e;

    assertColumnVectorEq<Int64>(expected, result_block, "t2.attr");
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}
