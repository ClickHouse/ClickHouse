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
        test<Key, ::absl::flat_hash_map<Key, UInt64, DefaultHash<Key>>>(data.data(), data.size(), "Abseil HashMap with CH Hash");
    }
    else if (method == 4)
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
 * ./integer_hash_tables_benchmark 4 $2 100000000 < $1
 *
 * Results of this benchmark on hits_100m_obfuscated X86-64
 *
 * File hits_100m_obfuscated/201307_1_96_4/WatchID.bin
 * CH HashMap: Elapsed: 7.416 (13484217.815 elem/sec.), map size: 99997493
 * Google DenseMap: Elapsed: 10.303 (9706022.031 elem/sec.), map size: 99997493
 * Abseil HashMap: Elapsed: 9.106 (10982139.229 elem/sec.), map size: 99997493
 * Abseil HashMap with CH Hash: Elapsed: 9.221 (10845360.669 elem/sec.), map size: 99997493
 * std::unordered_map: Elapsed: 45.213 (2211758.706 elem/sec.), map size: 9999749
 *
 * File hits_100m_obfuscated/201307_1_96_4/URLHash.bin
 * CH HashMap: Elapsed: 2.620 (38168135.308 elem/sec.), map size: 20714865
 * Google DenseMap: Elapsed: 3.426 (29189309.058 elem/sec.), map size: 20714865
 * Abseil HashMap: Elapsed: 2.788 (35870495.097 elem/sec.), map size: 20714865
 * Abseil HashMap with CH Hash: Elapsed: 2.991 (33428850.155 elem/sec.), map size: 20714865
 * std::unordered_map: Elapsed: 8.503 (11760331.346 elem/sec.), map size: 20714865
 *
 * File hits_100m_obfuscated/201307_1_96_4/UserID.bin
 * CH HashMap: Elapsed: 2.157 (46352039.753 elem/sec.), map size: 17630976
 * Google DenseMap: Elapsed: 2.725 (36694226.782 elem/sec.), map size: 17630976
 * Abseil HashMap: Elapsed: 2.590 (38604284.187 elem/sec.), map size: 17630976
 * Abseil HashMap with CH Hash: Elapsed: 2.785 (35904856.137 elem/sec.), map size: 17630976
 * std::unordered_map: Elapsed: 7.268 (13759557.609 elem/sec.), map size: 17630976
 *
 * File hits_100m_obfuscated/201307_1_96_4/RegionID.bin
 * CH HashMap: Elapsed: 0.192 (521583315.810 elem/sec.), map size: 9040
 * Google DenseMap: Elapsed: 0.297 (337081407.799 elem/sec.), map size: 9046
 * Abseil HashMap: Elapsed: 0.295 (338805623.511 elem/sec.), map size: 9040
 * Abseil HashMap with CH Hash: Elapsed: 0.331 (302155391.036 elem/sec.), map size: 9040
 * std::unordered_map: Elapsed: 0.455 (219971555.390 elem/sec.), map size: 9040
 *
 * File hits_100m_obfuscated/201307_1_96_4/CounterID.bin
 * CH HashMap: Elapsed: 0.217 (460216823.609 elem/sec.), map size: 6506
 * Google DenseMap: Elapsed: 0.373 (267838665.098 elem/sec.), map size: 6506
 * Abseil HashMap: Elapsed: 0.325 (308124728.989 elem/sec.), map size: 6506
 * Abseil HashMap with CH Hash: Elapsed: 0.354 (282167144.801 elem/sec.), map size: 6506
 * std::unordered_map: Elapsed: 0.390 (256573354.171 elem/sec.), map size: 6506
 *
 * File hits_100m_obfuscated/201307_1_96_4/TraficSourceID.bin
 * CH HashMap: Elapsed: 0.246 (406714566.282 elem/sec.), map size: 10
 * Google DenseMap: Elapsed: 0.760 (131615151.233 elem/sec.), map size: 1565609 /// Broken because there is 0 key in dataset
 * Abseil HashMap: Elapsed: 0.309 (324068156.680 elem/sec.), map size: 10
 * Abseil HashMap with CH Hash: Elapsed: 0.339 (295108223.814 elem/sec.), map size: 10
 * std::unordered_map: Elapsed: 0.811 (123304031.195 elem/sec.), map size: 10
 *
 * File hits_100m_obfuscated/201307_1_96_4/AdvEngineID.bin
 * CH HashMap: Elapsed: 0.155 (643245257.748 elem/sec.), map size: 19
 * Google DenseMap: Elapsed: 1.629 (61395025.417 elem/sec.), map size: 32260732 // Broken because there is 0 key in dataset
 * Abseil HashMap: Elapsed: 0.292 (342765027.204 elem/sec.), map size: 19
 * Abseil HashMap with CH Hash: Elapsed: 0.330 (302822020.210 elem/sec.), map size: 19
 * std::unordered_map: Elapsed: 0.308 (325059333.730 elem/sec.), map size: 19
 *
 *
 * Results of this benchmark on hits_100m_obfuscated AARCH64
 *
 * File hits_100m_obfuscated/201307_1_96_4/WatchID.bin
 * CH HashMap: Elapsed: 9.530 (10493528.533 elem/sec.), map size: 99997493
 * Google DenseMap: Elapsed: 14.436 (6927091.135 elem/sec.), map size: 99997493
 * Abseil HashMap: Elapsed: 16.671 (5998504.085 elem/sec.), map size: 99997493
 * Abseil HashMap with CH Hash: Elapsed: 16.803 (5951365.711 elem/sec.), map size: 99997493
 * std::unordered_map: Elapsed: 50.805 (1968305.658 elem/sec.), map size: 99997493
 *
 * File hits_100m_obfuscated/201307_1_96_4/URLHash.bin
 * CH HashMap: Elapsed: 3.693 (27076878.092 elem/sec.), map size: 20714865
 * Google DenseMap: Elapsed: 5.051 (19796401.694 elem/sec.), map size: 20714865
 * Abseil HashMap: Elapsed: 5.617 (17804528.625 elem/sec.), map size: 20714865
 * Abseil HashMap with CH Hash: Elapsed: 5.702 (17537013.639 elem/sec.), map size: 20714865
 * std::unordered_map: Elapsed: 10.757 (9296040.953 elem/sec.), map size: 2071486
 *
 * File hits_100m_obfuscated/201307_1_96_4/UserID.bin
 * CH HashMap: Elapsed: 2.982 (33535795.695 elem/sec.), map size: 17630976
 * Google DenseMap: Elapsed: 3.940 (25381557.959 elem/sec.), map size: 17630976
 * Abseil HashMap: Elapsed: 4.493 (22259078.458 elem/sec.), map size: 17630976
 * Abseil HashMap with CH Hash: Elapsed: 4.596 (21759738.710 elem/sec.), map size: 17630976
 * std::unordered_map: Elapsed: 9.035 (11067903.596 elem/sec.), map size: 17630976
 *
 * File hits_100m_obfuscated/201307_1_96_4/RegionID.bin
 * CH HashMap: Elapsed: 0.302 (331026285.361 elem/sec.), map size: 9040
 * Google DenseMap: Elapsed: 0.623 (160419421.840 elem/sec.), map size: 9046
 * Abseil HashMap: Elapsed: 0.981 (101971186.758 elem/sec.), map size: 9040
 * Abseil HashMap with CH Hash: Elapsed: 0.991 (100932993.199 elem/sec.), map size: 9040
 * std::unordered_map: Elapsed: 0.809 (123541402.715 elem/sec.), map size: 9040
 *
 * File hits_100m_obfuscated/201307_1_96_4/CounterID.bin
 * CH HashMap: Elapsed: 0.343 (291821742.078 elem/sec.), map size: 6506
 * Google DenseMap: Elapsed: 0.718 (139191105.450 elem/sec.), map size: 6506
 * Abseil HashMap: Elapsed: 1.019 (98148285.278 elem/sec.), map size: 6506
 * Abseil HashMap with CH Hash: Elapsed: 1.048 (95446843.667 elem/sec.), map size: 6506
 * std::unordered_map: Elapsed: 0.701 (142701070.085 elem/sec.), map size: 6506
 *
 * File hits_100m_obfuscated/201307_1_96_4/TraficSourceID.bin
 * CH HashMap: Elapsed: 0.376 (265905243.103 elem/sec.), map size: 10
 * Google DenseMap: Elapsed: 1.309 (76420707.298 elem/sec.), map size: 1565609 /// Broken because there is 0 key in dataset
 * Abseil HashMap: Elapsed: 0.955 (104668109.775 elem/sec.), map size: 10
 * Abseil HashMap with CH Hash: Elapsed: 0.967 (103456305.391 elem/sec.), map size: 10
 * std::unordered_map: Elapsed: 1.241 (80591305.890 elem/sec.), map size: 10
 *
 * File hits_100m_obfuscated/201307_1_96_4/AdvEngineID.bin
 * CH HashMap: Elapsed: 0.213 (470208130.105 elem/sec.), map size: 19
 * Google DenseMap: Elapsed: 2.525 (39607131.523 elem/sec.), map size: 32260732 /// Broken because there is 0 key in dataset
 * Abseil HashMap: Elapsed: 0.950 (105233678.618 elem/sec.), map size: 19
 * Abseil HashMap with CH Hash: Elapsed: 0.962 (104001230.717 elem/sec.), map size: 19
 * std::unordered_map: Elapsed: 0.585 (171059989.837 elem/sec.), map size: 19
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
