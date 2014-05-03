#include <iostream>
#include <iomanip>
#include <vector>

#include <unordered_map>

#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>

#include <statdaemons/Stopwatch.h>

//#define DBMS_HASH_MAP_COUNT_COLLISIONS
#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <DB/Core/Types.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/Common/HashTable/HashMap.h>


typedef UInt64 Key;
typedef UInt64 Value;

struct CellWithoutZeroWithSavedHash : public HashMapCell<Key, Value, DefaultHash<Key> >
{
//	size_t saved_hash;

	static constexpr bool need_zero_value_storage = false;

	CellWithoutZeroWithSavedHash() : HashMapCell() {}
	CellWithoutZeroWithSavedHash(const Key & key_, const State & state) : HashMapCell(key_, state) {}
	CellWithoutZeroWithSavedHash(const value_type & value_, const State & state) : HashMapCell(value_, state) {}

/*	bool keyEquals(const Key & key_) const { return value.first == key_; }
	bool keyEquals(const CellWithoutZeroWithSavedHash & other) const { return saved_hash == other.saved_hash && value.first == other.value.first; }

	void setHash(size_t hash_value) { saved_hash = hash_value; }
	size_t getHash(const DefaultHash<Key> & hash) const { return saved_hash; }*/
};

struct Grower : public HashTableGrower<>
{
	/// Состояние этой структуры достаточно, чтобы получить размер буфера хэш-таблицы.

	/// Определяет начальный размер хэш-таблицы.
	static const size_t initial_size_degree = 16;
	Grower() { size_degree = initial_size_degree; }

//	size_t max_fill = (1 << initial_size_degree) * 0.9;

	/// Размер хэш-таблицы в ячейках.
	size_t bufSize() const				{ return 1 << size_degree; }

	size_t maxFill() const				{ return 1 << (size_degree - 1); }
//	size_t maxFill() const				{ return max_fill; }

	size_t mask() const					{ return bufSize() - 1; }

	/// Из значения хэш-функции получить номер ячейки в хэш-таблице.
	size_t place(size_t x) const 		{ return x & mask(); }

	/// Следующая ячейка в цепочке разрешения коллизий.
	size_t next(size_t pos) const		{ ++pos; return pos & mask(); }

	/// Является ли хэш-таблица достаточно заполненной. Нужно увеличить размер хэш-таблицы, или удалить из неё что-нибудь ненужное.
	bool overflow(size_t elems) const	{ return elems > maxFill(); }

	/// Увеличить размер хэш-таблицы.
	void increaseSize()
	{
		size_degree += size_degree >= 23 ? 1 : 2;
//		max_fill = (1 << size_degree) * 0.9;
	}

	/// Установить размер буфера по количеству элементов хэш-таблицы. Используется при десериализации хэш-таблицы.
	void set(size_t num_elems)
	{
		throw Poco::Exception(__PRETTY_FUNCTION__);
	}
};


int main(int argc, char ** argv)
{
	size_t n = atoi(argv[1]);
	size_t m = atoi(argv[2]);

	std::vector<Key> data(n);

	std::cerr << "sizeof(Key) = " << sizeof(Key) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

	{
		Stopwatch watch;
		DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
		DB::CompressedReadBuffer in2(in1);

		in2.readStrict(reinterpret_cast<char*>(&data[0]), sizeof(data[0]) * n);

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Vector. Size: " << n
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	if (m == 1)
	{
		Stopwatch watch;

//		typedef HashMap<Key, Value> Map;

		/// Из-за WithoutZero быстрее на 0.7% (для не влезающей в L3-кэш) - 2.3% (для влезающей в L3-кэш).
		typedef HashMapTable<Key, CellWithoutZeroWithSavedHash, DefaultHash<Key>, Grower> Map;

		Map map;
		Map::iterator it;
		bool inserted;

		for (size_t i = 0; i < n; ++i)
		{
			map.emplace(data[i], it, inserted);
			if (inserted)
				it->second = 0;
			++it->second;
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "HashMap. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
			<< ", collisions: " << map.getCollisions()
#endif
			<< std::endl;
	}

	if (m == 2)
	{
		Stopwatch watch;

		std::unordered_map<Key, Value, DefaultHash<Key> > map;
		for (size_t i = 0; i < n; ++i)
			++map[data[i]];
		
		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "std::unordered_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	if (m == 3)
	{
		Stopwatch watch;

		google::dense_hash_map<Key, Value, DefaultHash<Key> > map;
		map.set_empty_key(-1ULL);
		for (size_t i = 0; i < n; ++i)
  			++map[data[i]];
		
		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "google::dense_hash_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	if (m == 4)
	{
		Stopwatch watch;

		google::sparse_hash_map<Key, Value, DefaultHash<Key> > map;
		for (size_t i = 0; i < n; ++i)
			++map[data[i]];

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "google::sparse_hash_map. Size: " << map.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	return 0;
}
