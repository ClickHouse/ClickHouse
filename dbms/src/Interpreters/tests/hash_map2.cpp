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


struct StringZeroTraits
{
	static inline bool check(const std::string & x) { return x.empty(); }
	static inline void set(std::string & x) { x.clear(); }
};


/// Немного быстрее стандартного
struct StringHash
{
	size_t operator()(const std::string & x) const { return CityHash64(x.data(), x.size()); }
};


int main(int argc, char ** argv)
{
	size_t n = 100000;
	std::vector<std::string> data(n);

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
			<< "Size: " << n
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

/*	{
		Stopwatch watch;

		DB::HashMap<std::string, DB::UInt64, StringHash, StringZeroTraits> map;
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], 0));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}*/

	{
		Stopwatch watch;

		std::tr1::unordered_map<std::string, DB::UInt64> map;
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], 0));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	/*{
		Stopwatch watch;

		google::dense_hash_map<DB::UInt64, DB::UInt64> map;
		map.set_empty_key(0);
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], 0));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}*/

	/*{
		Stopwatch watch;

		google::sparse_hash_map<DB::UInt64, DB::UInt64> map;
		for (size_t i = 0; i < n; ++i)
			map.insert(std::make_pair(data[i], 0));

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}*/
	
	return 0;
}
