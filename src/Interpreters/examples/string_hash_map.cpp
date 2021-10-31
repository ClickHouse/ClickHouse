#include <iomanip>
#include <iostream>
#include <vector>
#include <Compression/CompressedReadBuffer.h>
#include <base/types.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/Stopwatch.h>
#include <base/StringRef.h>

/**

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
for file in term1 term2 term4 term8 term16 term24 term48; do
    for size in 30000000 50000000 80000000 100000000; do
        BEST_METHOD=0
        BEST_RESULT=0
        for method in {1..2}; do
            echo -ne $file $size $method ''
            numactl --membind=0 taskset -c 18 ./string_hash_map $size $method <"$dir"/"$file".bin 2>&1 | perl -nE 'say /([0-9\.]+) elem/g if /HashMap/' | tee /tmp/string_hash_map_res
            CUR_RESULT=$(cat /tmp/string_hash_map_res | tr -d '.')
            if [[ $CUR_RESULT -gt $BEST_RESULT ]]; then
                BEST_METHOD=$method
                BEST_RESULT=$CUR_RESULT
            fi
        done
        echo Best: $BEST_METHOD - $BEST_RESULT
    done
done

---------------------------

term1 30000000 1 68785770.85   term2 30000000 1 42531788.83   term4 30000000 1 14759901.41   term8 30000000 1 8072903.47
term1 30000000 2 40812128.16   term2 30000000 2 21352402.10   term4 30000000 2 9008907.80    term8 30000000 2 5822641.82
Best: 1 - 6878577085           Best: 1 - 4253178883           Best: 1 - 1475990141           Best: 1 - 807290347
term1 50000000 1 68027542.41   term2 50000000 1 40493742.80   term4 50000000 1 16827650.85   term8 50000000 1 7405230.14
term1 50000000 2 37589806.02   term2 50000000 2 19362975.09   term4 50000000 2 8278094.11    term8 50000000 2 5106810.80
Best: 1 - 6802754241           Best: 1 - 4049374280           Best: 1 - 1682765085           Best: 1 - 740523014
term1 80000000 1 68651875.88   term2 80000000 1 38253695.50   term4 80000000 1 15847177.93   term8 80000000 1 7536319.25
term1 80000000 2 38092189.20   term2 80000000 2 20287003.01   term4 80000000 2 9322770.34    term8 80000000 2 4355572.15
Best: 1 - 6865187588           Best: 1 - 3825369550           Best: 1 - 1584717793           Best: 1 - 753631925
term1 100000000 1 68641941.59  term2 100000000 1 39120834.79  term4 100000000 1 16773904.90  term8 100000000 1 7147146.55
term1 100000000 2 38358006.72  term2 100000000 2 20629363.17  term4 100000000 2 9665201.92   term8 100000000 2 4728255.07
Best: 1 - 6864194159           Best: 1 - 3912083479           Best: 1 - 1677390490           Best: 1 - 714714655


term16 30000000 1 6823029.35        term24 30000000 1 5706271.14        term48 30000000 1 4695716.47
term16 30000000 2 5672283.33        term24 30000000 2 5498855.56        term48 30000000 2 4860537.26
Best: 1 - 682302935                 Best: 1 - 570627114                 Best: 2 - 486053726
term16 50000000 1 6214581.25        term24 50000000 1 5249785.66        term48 50000000 1 4282606.12
term16 50000000 2 4990361.44        term24 50000000 2 4855552.24        term48 50000000 2 4348923.29
Best: 1 - 621458125                 Best: 1 - 524978566                 Best: 2 - 434892329
term16 80000000 1 5382855.70        term24 80000000 1 4580133.04        term48 80000000 1 3779436.15
term16 80000000 2 4282192.79        term24 80000000 2 4178791.09        term48 80000000 2 3788409.72
Best: 1 - 538285570                 Best: 1 - 458013304                 Best: 2 - 378840972
term16 100000000 1 5930103.42       term24 100000000 1 5030621.52       term48 100000000 1 4084666.73
term16 100000000 2 4621719.60       term24 100000000 2 4499866.83       term48 100000000 2 4067029.31
Best: 1 - 593010342                 Best: 1 - 503062152                 Best: 1 - 408466673

*/


using Value = uint64_t;

template <typename Map>
void NO_INLINE bench(const std::vector<StringRef> & data, DB::Arena &, const char * name)
{
    // warm up
    /*
    {
        Map map;
        typename Map::LookupResult it;
        bool inserted;

        for (size_t i = 0, size = data.size(); i < size; ++i)
        {
            auto key_holder = DB::ArenaKeyHolder{data[i], pool};
            map.emplace(key_holder, it, inserted);
            if (inserted)
                it->getSecond() = 0;
            ++it->getSecond();
        }
    }
    */

    std::cerr << "method " << name << std::endl;
    for (auto t = 0ul; t < 7; ++t)
    {
        DB::Arena pool(128 * 1024 * 1024);
        Stopwatch watch;
        Map map;
        typename Map::LookupResult it;
        bool inserted;

        for (const auto & value : data)
        {
            map.emplace(DB::ArenaKeyHolder{value, pool}, it, inserted);
            if (inserted)
                it->getMapped() = 0;
            ++it->getMapped();
        }
        watch.stop();

        std::cerr << "arena-memory " << pool.size() + map.getBufferSizeInBytes() << std::endl;
        std::cerr << "single-run " << std::setprecision(3)
                  << watch.elapsedSeconds() << std::endl;
    }
}

/*
template <typename Map>
runFromFile()
{
    DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
    DB::CompressedReadBuffer in2(in1);

    Map map;
    DB::Arena pool(128 * 1024 * 1024);
    for (size_t i = 0; i < n && !in2.eof(); ++i)
    {
        auto key = DB::readStringBinaryInto(pool, in2);

        bool inserted;
        Map::LookupResult mapped;
        map.emplaceKeyHolder(DB::SerializedKeyHolder(key, pool), mapped, inserted);
    }
}

template <typename Map>
benchFromFile()
{
    double best_time = -1.;
    for (auto t = 0ul; t < 50; ++t)
    {
        Stopwatch watch;
        runFromFile();
        watch.stop();

        if (best_time < 0 || best_time > watch.elapsedSeconds())
        {
            best_time = watch.elapsedSeconds();
        }
    }

    std::cerr << std::fixed << std::setprecision(2) << "HashMap (" << name << "). Elapsed: " << best_time << " (" << data.size() / best_time
              << " elem/sec.)" << std::endl;
}
*/


int main(int argc, char ** argv)
{
    if (argc < 3)
    {
        std::cerr << "Usage: program n m\n";
        return 1;
    }

    size_t n = std::stol(argv[1]);
    size_t m = std::stol(argv[2]);

    DB::Arena pool(128 * 1024 * 1024);
    std::vector<StringRef> data(n);

    std::cerr << "sizeof(Key) = " << sizeof(StringRef) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

    {
        Stopwatch watch;
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);

        std::string tmp;
        for (size_t i = 0; i < n && !in2.eof(); ++i)
        {
            DB::readStringBinary(tmp, in2);
            data[i] = StringRef(pool.insert(tmp.data(), tmp.size()), tmp.size());
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2) << "Vector. Size: " << n << ", elapsed: " << watch.elapsedSeconds() << " ("
                  << n / watch.elapsedSeconds() << " elem/sec.)" << std::endl;
    }

    if (!m || m == 1)
        bench<StringHashMap<Value>>(data, pool, "StringHashMap");
    if (!m || m == 2)
        bench<HashMapWithSavedHash<StringRef, Value>>(data, pool, "HashMapWithSavedHash");
    if (!m || m == 3)
        bench<HashMap<StringRef, Value>>(data, pool, "HashMap");
    return 0;
}
