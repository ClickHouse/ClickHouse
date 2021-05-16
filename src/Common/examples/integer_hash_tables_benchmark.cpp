#include <iostream>
#include <iomanip>
#include <vector>

#include <unordered_map>

#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>
#include <absl/container/flat_hash_map.h>

#include <Common/Stopwatch.h>

#include <common/types.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <Common/HashTable/HashMap.h>

template <typename Key, typename Map>
void NO_INLINE test(const Key * data, size_t size, const std::string & name, std::function<void(Map &)> init = {})
{
    Stopwatch watch;

    Map map;

    if (init)
        init(map);

    for (const auto * end = data + size; data < end; ++data)
        ++map[*data];

    watch.stop();
    std::cerr << name
        << ":\nElapsed: " << watch.elapsedSeconds()
        << " (" << size / watch.elapsedSeconds() << " elem/sec.)"
        << ", map size: " << map.size() << "\n";
}

template <typename Key>
static void NO_INLINE testForType(size_t method, size_t rows_size)
{
    std::cerr << std::fixed << std::setprecision(3);

    std::vector<Key> data(rows_size);

    {
        DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        DB::CompressedReadBuffer in2(in1);
        in2.readStrict(reinterpret_cast<char*>(data.data()), sizeof(data[0]) * rows_size);
    }

    if (method == 0)
    {
        test<Key, HashMap<Key, UInt64, DefaultHash<Key>>>(data.data(), data.size(), "CH HashMap");
    }
    else if (method == 1)
    {
        test<Key, ::google::dense_hash_map<Key, UInt64, absl::Hash<Key>>>(data.data(), data.size(), "Google DenseMap", [](auto & map){ map.set_empty_key(0); });
    }
    else if (method == 2)
    {
        test<Key, ::absl::flat_hash_map<Key, UInt64>>(data.data(), data.size(), "Abseil HashMap");
    }
    else if (method == 3)
    {
        test<Key, std::unordered_map<Key, UInt64>>(data.data(), data.size(), "std::unordered_map");
    }
    else
    {
        std::cerr << "Unexpected method passed " << method << std::endl;
    }
}

/** This benchmark does not test which hash table is fastest.
 * It tests simple aggregation scenario that is important for ClickHouse.
 *
 * Support bash script it is important to rerun program for each method:
 * benchmark.sh
 * # Usage benchmark.sh column_file_name.bin column_type
 * echo File $1
 * ./integer_hash_tables_benchmark 0 $2 100000000 < $1
 * ./integer_hash_tables_benchmark 1 $2 100000000 < $1
 * ./integer_hash_tables_benchmark 2 $2 100000000 < $1
 * ./integer_hash_tables_benchmark 3 $2 100000000 < $1
 *
 * Results of this benchmark on hits_100m_obfuscated
 *
 * File hits_100m_obfuscated/201307_1_96_4/WatchID.bin
 * CH HashMap: Elapsed: 7.366 (13575745.933 elem/sec.), map size: 99997493
 * Google DenseMap: Elapsed: 10.089 (9911817.125 elem/sec.), map size: 99997493
 * Abseil HashMap: Elapsed: 9.011 (11097794.073 elem/sec.), map size: 99997493
 * std::unordered_map: Elapsed: 44.758 (2234223.189 elem/sec.), map size: 99997493
 *
 * File hits_100m_obfuscated/201307_1_96_4/URLHash.bin
 * CH HashMap: Elapsed: 2.672 (37421588.347 elem/sec.), map size: 20714865
 * Google DenseMap: Elapsed: 3.409 (29333308.209 elem/sec.), map size: 20714865
 * Abseil HashMap: Elapsed: 2.778 (36000540.035 elem/sec.), map size: 20714865
 * std::unordered_map: Elapsed: 8.643 (11570012.207 elem/sec.), map size: 20714865
 *
 * File hits_100m_obfuscated/201307_1_96_4/UserID.bin
 * CH HashMap: Elapsed: 2.116 (47267659.076 elem/sec.), map size: 17630976
 * Google DenseMap: Elapsed: 2.722 (36740693.786 elem/sec.), map size: 17630976
 * Abseil HashMap: Elapsed: 2.597 (38509988.663 elem/sec.), map size: 17630976
 * std::unordered_map: Elapsed: 7.327 (13647271.471 elem/sec.), map size: 17630976
 *
 * File hits_100m_obfuscated/201307_1_96_4/RegionID.bin
 * CH HashMap: Elapsed: 0.201 (498144193.695 elem/sec.), map size: 9040
 * Google DenseMap: Elapsed: 0.261 (382656387.016 elem/sec.), map size: 9046
 * Abseil HashMap: Elapsed: 0.307 (325874545.117 elem/sec.), map size: 9040
 * std::unordered_map: Elapsed: 0.466 (214379083.420 elem/sec.), map size: 9040
 *
 * File hits_100m_obfuscated/201307_1_96_4/CounterID.bin
 * CH HashMap: Elapsed: 0.220 (455344735.648 elem/sec.), map size: 6506
 * Google DenseMap: Elapsed: 0.297 (336187522.818 elem/sec.), map size: 6506
 * Abseil HashMap: Elapsed: 0.307 (325264214.480 elem/sec.), map size: 6506
 * std::unordered_map: Elapsed: 0.389 (257195996.114 elem/sec.), map size: 6506
 *
 * File hits_100m_obfuscated/201307_1_96_4/TraficSourceID.bin
 * CH HashMap: Elapsed: 0.274 (365196673.729 elem/sec.), map size: 10
 * Google DenseMap: Elapsed: 0.782 (127845746.927 elem/sec.), map size: 1565609 /// Broken because there is 0 key in dataset
 * Abseil HashMap: Elapsed: 0.303 (330461565.053 elem/sec.), map size: 10
 * std::unordered_map: Elapsed: 0.843 (118596530.649 elem/sec.), map size: 10
 *
 * File hits_100m_obfuscated/201307_1_96_4/AdvEngineID.bin
 * CH HashMap: Elapsed: 0.160 (623399865.019 elem/sec.), map size: 19
 * Google DenseMap: Elapsed: 1.673 (59757144.027 elem/sec.), map size: 32260732 /// Broken because there is 0 key in dataset
 * Abseil HashMap: Elapsed: 0.297 (336589258.845 elem/sec.), map size: 19
 * std::unordered_map: Elapsed: 0.332 (301114451.384 elem/sec.), map size: 19
 */

int main(int argc, char ** argv)
{
    if (argc < 4)
    {
        std::cerr << "Usage: program method column_type_name rows_count < input_column.bin \n";
        return 1;
    }

    size_t method = std::stoull(argv[1]);
    std::string type_name = std::string(argv[2]);
    size_t n = std::stoull(argv[3]);

    if (type_name == "UInt8")
        testForType<UInt8>(method, n);
    else if (type_name == "UInt16")
        testForType<UInt16>(method, n);
    else if (type_name == "UInt32")
        testForType<UInt32>(method, n);
    else if (type_name == "UInt64")
        testForType<UInt64>(method, n);
    else if (type_name == "Int8")
        testForType<Int8>(method, n);
    else if (type_name == "Int16")
        testForType<Int16>(method, n);
    else if (type_name == "Int32")
        testForType<Int32>(method, n);
    else if (type_name == "Int64")
        testForType<Int64>(method, n);
    else
        std::cerr << "Unexpected type passed " << type_name << std::endl;

    return 0;
}
