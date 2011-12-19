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


struct SimpleString
{
	char * data;

	SimpleString() : data(NULL) {}

	SimpleString(const std::string & s)
	{
		data = reinterpret_cast<char *>(malloc(s.size()));
		memcpy(data, s.data(), s.size());
	}

 	SimpleString(const SimpleString & s)
	{
		/// move
		data = s.data;
		const_cast<char *&>(s.data) = NULL;
	}

	~SimpleString()
	{
		free(data);
	}

	bool operator== (const SimpleString & s)
	{
		return 0 == strcmp(data, s.data);
	}

	bool operator!= (const SimpleString & s)
	{
		return !operator==(s);
	}
};

struct SimpleStringZeroTraits
{
	static inline bool check(const SimpleString & x) { return 0 == x.data; }
	static inline void set(SimpleString & x) { x.data = 0; }
};

struct SimpleStringHash
{
    size_t operator()(const SimpleString & x) const { return CityHash64(x.data, strlen(x.data)); }
};


int main(int argc, char ** argv)
{
	typedef DB::String Key;
	typedef DB::AggregateFunctionsPlainPtrs Value;

	DB::AggregateFunctionFactory factory;
	DB::DataTypes data_types_empty;
	DB::DataTypes data_types_uint64;
	data_types_uint64.push_back(new DB::DataTypeUInt64);

	size_t n = 1000000;
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

		typedef DB::HashMap<SimpleString, Value, SimpleStringHash, SimpleStringZeroTraits> Map;
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

