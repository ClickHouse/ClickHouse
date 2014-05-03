#include <iostream>
#include <iomanip>
#include <vector>

#include <statdaemons/Stopwatch.h>

#include <DB/Common/HashTable/Hash.h>
#include <DB/Common/HashTable/HashTable.h>
#include <DB/Common/HashTable/HashTableMerge.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>


typedef UInt64 Key;
typedef UInt64 Value;


template
<
	typename Key,
	typename Cell,
	typename Hash,
	typename Grower,
	typename Allocator = HashTableAllocator
>
class HashTableWithDump : public HashTable<Key, Cell, Hash, Grower, Allocator>
{
public:
	void dump() const
	{
		for (size_t i = 0; i < this->grower.bufSize(); ++i)
		{
			if (this->buf[i].isZero(*this))
				std::cerr << "[    ]";
			else
				std::cerr << '[' << std::right << std::setw(4) << this->buf[i].getValue() << ']';
		}
		std::cerr << std::endl;
	}
};


struct TrivialHash
{
	size_t operator() (UInt64 x) const { return x; }
};


struct Grower
{
	/// Состояние этой структуры достаточно, чтобы получить размер буфера хэш-таблицы.

	/// Определяет начальный размер хэш-таблицы.
	static const size_t initial_size_degree = 4;

	UInt8 size_degree = initial_size_degree;

	/// Размер хэш-таблицы в ячейках.
	size_t bufSize() const				{ return 1 << size_degree; }

	size_t maxFill() const				{ return 1 << (size_degree - 1); }
	size_t mask() const					{ return bufSize() - 1; }

	/// Из значения хэш-функции получить номер ячейки в хэш-таблице.
	size_t place(size_t x) const 		{ return x & mask(); }

	/// Следующая ячейка в цепочке разрешения коллизий.
	size_t next(size_t pos) const		{ ++pos; return pos & mask(); }

	/// Является ли хэш-таблица достаточно заполненной. Нужно увеличить размер хэш-таблицы, или удалить из неё что-нибудь ненужное.
	bool overflow(size_t elems) const	{ return false; }

	/// Увеличить размер хэш-таблицы.
	void increaseSize()
	{
		size_degree += size_degree >= 23 ? 1 : 2;
	}

	/// Установить размер буфера по количеству элементов хэш-таблицы. Используется при десериализации хэш-таблицы.
	void set(size_t num_elems)
	{
		size_degree = num_elems <= 1
			 ? initial_size_degree
			 : ((initial_size_degree > static_cast<size_t>(log2(num_elems - 1)) + 2)
				 ? initial_size_degree
				 : (static_cast<size_t>(log2(num_elems - 1)) + 2));
	}
};


typedef HashTableWithDump<Key, HashTableCell<Key, TrivialHash>, TrivialHash, HashTableGrower, HashTableAllocator> Set;


int main(int argc, char ** argv)
{
/*	Set set;

	set.dump();
	set.insert(37);
	set.dump();
	set.insert(21);
	set.dump();
	set.insert(5);
	set.dump();
	set.insert(6);
	set.dump();
	set.insert(22);
	set.dump();

	set.insert(14);
	set.dump();
	set.insert(15);
	set.dump();
	set.insert(30);
	set.dump();
	set.insert(1);
	set.dump();

	std::cerr << std::endl;
	for (HashTableMergeCursor<Set> it(&set); it.isValid(); it.next())
		std::cerr << it.get() << std::endl;

	set.resize(15);
	set.dump();

	std::cerr << std::endl;
	for (HashTableMergeCursor<Set> it(&set); it.isValid(); it.next())
		std::cerr << it.get() << std::endl;

	*/

	size_t n = atoi(argv[1]);
	std::vector<Key> data(n);

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

	{
		Stopwatch watch;

		Set set;

		for (size_t i = 0; i < n; ++i)
			set.insert(data[i]);

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "HashSet. Size: " << set.size()
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;

		{
			watch.restart();

			size_t sum = 0;
			for (auto x : set)
				sum += x;

			watch.stop();
			std::cerr << std::fixed << std::setprecision(2)
				<< "Iterated in: " << watch.elapsedSeconds()
				<< " (" << set.size() / watch.elapsedSeconds() << " elem/sec.)"
				<< " sum = " << sum
				<< std::endl;
		}

		{
			watch.restart();

			size_t sum = 0;
			for (HashTableMergeCursor<Set> it(&set); it.isValid(); it.next())
				sum += it.get();

			watch.stop();
			std::cerr << std::fixed << std::setprecision(2)
				<< "Ordered iterated in: " << watch.elapsedSeconds()
				<< " (" << set.size() / watch.elapsedSeconds() << " elem/sec.)"
				<< " sum = " << sum
				<< std::endl;
		}
	}

	return 0;
}
