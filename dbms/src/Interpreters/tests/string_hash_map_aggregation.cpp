#include <iomanip>
#include <iostream>
#include <vector>
#include <Columns/ColumnString.h>
#include <Compression/CompressedReadBuffer.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Types.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/Aggregator.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/Stopwatch.h>


/*

#include <fstream>
#include <random>

using namespace std;

int main()
{
    std::string s;
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, 25);
    std::binomial_distribution<std::mt19937::result_type> binomial1(100, 0.01);
    std::binomial_distribution<std::mt19937::result_type> binomial2(100, 0.02);
    std::binomial_distribution<std::mt19937::result_type> binomial4(100, 0.04);
    std::binomial_distribution<std::mt19937::result_type> binomial8(100, 0.08);
    std::binomial_distribution<std::mt19937::result_type> binomial16(100, 0.16);
    std::binomial_distribution<std::mt19937::result_type> binomial24(100, 0.24);
    std::binomial_distribution<std::mt19937::result_type> binomial48(100, 0.48);
    // 11GB
    std::ofstream f("/tmp/terms.csv");
    size_t l1, l2, l4, l8, l16, l24, l48;
    for (auto n = 0ul; n < 1e8; ++n)
    {
        l1 = binomial1(rng) + 1;
        l2 = binomial2(rng) + l1 + 1;
        l4 = binomial4(rng) + l2 + 1;
        l8 = binomial8(rng) + l4 + 1;
        l16 = binomial16(rng) + l8 + 1;
        l24 = binomial24(rng) + l16 + 1;
        l48 = binomial48(rng) + l24 + 1;
        s.resize(l48);
        for (auto i = 0ul; i < l48 - 1; ++i)
            s[i] = 'a' + dist(rng);
        s[l1 - 1] = ',';
        s[l2 - 1] = ',';
        s[l4 - 1] = ',';
        s[l8 - 1] = ',';
        s[l16 - 1] = ',';
        s[l24 - 1] = ',';
        s[l48 - 1] = '\n';
        f << s;
    }
    f.close();
    return 0;
}

create table terms (term1 String, term2 String, term4 String, term8 String, term16 String, term24 String, term48 String) engine TinyLog;
insert into terms select * from file('/tmp/terms.csv', CSV, 'a String, b String, c String, d String, e String, f String, g String');

NOTE: for reliable test results, try isolating cpu cores and do python -m perf tune. Also bind numa nodes if any.
# isolate cpu 18
dir=/home/amos/git/chorigin/data/data/default/terms
export BEST
for file in term1 term2 term4 term8 term16 term24 term48; do
    for size in 30000000 50000000 80000000 100000000; do
        echo $file $size" rows"
        for method in {0..1}; do
            echo
            BEST=0
            for two_level in {0..1}; do
                for sso in {0..1}; do
                    printf $two_level$sso"  :  "
                    numactl --membind=0 taskset -c 18 ./string_hash_map_aggregation $size $method $two_level $sso <"$dir"/"$file".bin 2>&1 | tee /tmp/string_hash_map_res
                    BEST2=$(perl -nE '/,([0-9\.]+)/; print $1 > $ENV{"BEST"} ? $1 : $ENV{"BEST"};' < /tmp/string_hash_map_res)
                    if [ ! $BEST2 = $BEST ]; then
                        BEST=$BEST2
                        CUR=$two_level$sso
                    fi
                done
            done
            echo BEST IS $CUR : $BEST
        done
        echo
    done
done

*/

using namespace DB;

void bench_agg(Block & block, Block & firstblock, int two_level, int sso)
{
    size_t n = block.rows();
    AggregateDescriptions aggregate_descriptions(1);
    aggregate_descriptions[0].function = std::make_shared<AggregateFunctionCount>(DataTypes{});

    Aggregator::Params params(
        block.cloneEmpty(), {0}, aggregate_descriptions, false, 0, OverflowMode::THROW, 0, 0, 0, false, "", 1, 0, sso);

    // warm up
    {
        AggregatedDataVariants aggregated_data_variants;
        BlockInputStreamPtr stream = std::make_shared<ConcatBlockInputStream>(std::vector<BlockInputStreamPtr>{
            std::make_shared<OneBlockInputStream>(firstblock), std::make_shared<OneBlockInputStream>(block)});
        Aggregator aggregator(params);
        aggregator.execute(stream, aggregated_data_variants);
    }

    double best_time = 10000;
    for (auto i = 0ul; i < 5; ++i)
    {
        AggregatedDataVariants aggregated_data_variants;
        BlockInputStreamPtr stream = std::make_shared<ConcatBlockInputStream>(std::vector<BlockInputStreamPtr>{
            std::make_shared<OneBlockInputStream>(firstblock), std::make_shared<OneBlockInputStream>(block)});
        Stopwatch stopwatch;
        Aggregator aggregator(params);
        aggregator.execute(stream, aggregated_data_variants);
        best_time = std::min(best_time, stopwatch.elapsedSeconds());
    }
    const char * p[] = {"", "(two_level)", "(sso)", "(two_level, sso)"};
    std::cout << std::fixed << std::setprecision(2) << "Aggregation" << p[two_level + sso * 2] << " elapsed " << best_time << " sec.,"
              << n / best_time << " rows/sec. (best in 5)" << std::endl;
}

void bench_merge(Block & block, Block & firstblock, int two_level, int sso)
{
    size_t n = block.rows();
    AggregateDescriptions aggregate_descriptions(1);
    aggregate_descriptions[0].function = std::make_shared<AggregateFunctionCount>(DataTypes{});

    Aggregator::Params params(
        block.cloneEmpty(), {0}, aggregate_descriptions, false, 0, OverflowMode::THROW, 0, 0, 0, false, "", 1, 0, sso);

    // warm up
    {
        AggregatedDataVariantsPtr aggregated_data_variants_ptr = std::make_shared<AggregatedDataVariants>();
        AggregatedDataVariants & aggregated_data_variants = *aggregated_data_variants_ptr;
        BlockInputStreamPtr stream = std::make_shared<ConcatBlockInputStream>(std::vector<BlockInputStreamPtr>{
            std::make_shared<OneBlockInputStream>(firstblock), std::make_shared<OneBlockInputStream>(block)});
        Aggregator aggregator(params);
        aggregator.execute(stream, aggregated_data_variants);
        ManyAggregatedDataVariants many_data{aggregated_data_variants_ptr};
        auto impl = aggregator.mergeAndConvertToBlocks(many_data, true, 1);
        while (impl->read())
            ;
    }

    double best_time = 10000;
    for (auto i = 0ul; i < 5; ++i)
    {
        AggregatedDataVariantsPtr aggregated_data_variants_ptr = std::make_shared<AggregatedDataVariants>();
        AggregatedDataVariants & aggregated_data_variants = *aggregated_data_variants_ptr;
        BlockInputStreamPtr stream = std::make_shared<ConcatBlockInputStream>(std::vector<BlockInputStreamPtr>{
            std::make_shared<OneBlockInputStream>(firstblock), std::make_shared<OneBlockInputStream>(block)});
        Aggregator aggregator(params);
        aggregator.execute(stream, aggregated_data_variants);
        Stopwatch stopwatch;
        ManyAggregatedDataVariants many_data{aggregated_data_variants_ptr};
        auto impl = aggregator.mergeAndConvertToBlocks(many_data, true, 1);
        while (impl->read())
            ;
        best_time = std::min(best_time, stopwatch.elapsedSeconds());
    }
    const char * p[] = {"", "(two_level)", "(sso)", "(two_level, sso)"};
    std::cout << std::fixed << std::setprecision(2) << "Merge" << p[two_level + sso * 2] << " elapsed " << best_time << " sec.,"
              << n / best_time << " rows/sec. (best in 5)" << std::endl;
}

int main(int argc, char ** argv)
{
    if (argc < 5)
    {
        std::cerr << "Usage: program n kind two_level sso\n";
        return 1;
    }

    size_t n = atoi(argv[1]);
    int k = atoi(argv[2]);
    int t = atoi(argv[3]);
    int s = atoi(argv[4]);
    ReadBufferFromFileDescriptor in1(STDIN_FILENO);
    CompressedReadBuffer in2(in1);

    Block block;

    {
        ColumnWithTypeAndName column;
        column.name = "s";
        column.type = std::make_shared<DataTypeString>();
        auto col = ColumnString::create();
        column.type->deserializeBinaryBulk(*col, in2, n, 0);
        column.column = std::move(col);
        block.insert(column);
    }

    // a chance to convert to two level
    Block firstblock = block.cloneWithoutColumns();
    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
        firstblock.getByPosition(i).column = block.getByPosition(i).column->cut(0, 1);

    if (k)
        bench_merge(block, firstblock, t, s);
    else
        bench_agg(block, firstblock, t, s);
}
