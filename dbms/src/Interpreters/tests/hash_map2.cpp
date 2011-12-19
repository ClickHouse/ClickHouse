#include <iostream>
#include <iomanip>
#include <vector>

#include <tr1/unordered_map>

#include <city.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/Types.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/Interpreters/HashMap.h>


#include <iostream>
#include <iomanip>
#include <vector>

#include <tr1/unordered_map>

#include <google/dense_hash_map>
#include <google/sparse_hash_map>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/Types.h>
#include <DB/Interpreters/HashMap.h>
#include <DB/AggregateFunctions/IAggregateFunction.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


struct StringZeroTraits
{
	static char empty[sizeof(std::string)];

	static inline bool check(const std::string & x) { return 0 == memcmp(&x, &empty, sizeof(x)); }
	static inline void set(std::string & x) { memset(&x, 0, sizeof(x)); }
};

char StringZeroTraits::empty[sizeof(std::string)];


/// Немного быстрее стандартного
struct StringHash
{
    size_t operator()(const std::string & x) const { return CityHash64(x.data(), x.size()); }
};


int main(int argc, char ** argv)
{
	typedef DB::String Key;
	typedef DB::AggregateFunctions Value;

	DB::AggregateFunctionFactory factory;
	DB::DataTypes data_types_empty;
	DB::DataTypes data_types_uint64;
	data_types_uint64.push_back(new DB::DataTypeUInt64);

	size_t n = 100000;
	std::vector<Key> data(n);
	Value value;

	value.push_back(factory.get("count", data_types_empty));
	value.push_back(factory.get("avg", data_types_uint64));
	value.push_back(factory.get("uniq", data_types_uint64));

	std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

	{
        DB::ReadBufferFromFileDescriptor buf(0);
        Stopwatch watch;
        for (size_t i = 0; !buf.eof(); ++i)
        {
            DB::readEscapedString(data[i], buf);
            DB::assertString("\n", buf);
        }

        watch.stop();
        std::cerr << std::fixed << std::setprecision(2)
            << "Vector. Size: " << n
            << ", elapsed: " << watch.elapsedSeconds()
            << " (" << n / watch.elapsedSeconds() << " elem/sec.)"
            << std::endl;
    }

	{
		Stopwatch watch;

		typedef DB::HashMap<Key, Value, StringHash, StringZeroTraits> Map;
		Map map;
		Map::iterator it;
		bool inserted;

		for (size_t i = 0; i < n; ++i)
		{
			map.emplace(data[i], it, inserted);
			if (inserted)
				new(&it->second) Value(value);
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "DB::HashMap. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	{
		Stopwatch watch;

		std::tr1::unordered_map<Key, Value> map;
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], value));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "std::tr1::unordered_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	{
		Stopwatch watch;

		google::dense_hash_map<Key, Value> map;
		map.set_empty_key("");
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], value));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "google::dense_hash_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	/*{
		Stopwatch watch;

		google::sparse_hash_map<Key, Value> map;
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], value));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "google::sparse_hash_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}*/

	return 0;
}

