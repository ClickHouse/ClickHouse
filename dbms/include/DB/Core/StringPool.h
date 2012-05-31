#pragma once

#include <string.h>
#include <Yandex/optimization.h>


namespace DB
{


/** Пул, в который можно складывать короткие строки.
  * Сценарий использования:
  * - складываем много строк и запоминаем их адреса;
  * - адреса остаются валидными в течение жизни пула;
  * - при уничтожении пула, вся память освобождается;
  * - память выделяется и освобождается большими кусками;
  * - удаление части данных не предусмотрено;
  */
class StringPool
{
private:
	/// Непрерывный кусок памяти и указатель на свободное место в нём. Односвязный список.
	struct Chunk
	{
		char * begin;
		char * pos;
		char * end;

		Chunk * prev;

		Chunk(size_t size_, Chunk * prev_)
		{
			begin = new char[size_];
			pos = begin;
			end = begin + size_;
			prev = prev_;
		}

		~Chunk()
		{
			if (prev)
				delete prev;
			delete[] begin;
		}

		size_t size() { return end - begin; }
	};

	size_t growth_factor;
	/// Последний непрерывный кусок памяти.
	Chunk * head;

	/// Добавить следующий непрерывный кусок памяти размера не меньше заданного.
	void addChunk(size_t min_size)
	{
		if (unlikely(head->size() * growth_factor > min_size))
			min_size = head->size() * growth_factor;

		head = new Chunk(min_size, head);
	}

public:
	StringPool(size_t initial_size_ = 4096, size_t growth_factor_ = 2)
		: growth_factor(growth_factor_), head(new Chunk(initial_size_, NULL))
	{
	}

	~StringPool()
	{
		delete head;
	}

	/// Получить кусок памяти. (Полезно, если размер строки заранее известен, но вставить её нужно по частям.)
	char * alloc(size_t size)
	{
		if (unlikely(head->pos + size > head->end))
			addChunk(size);

		char * res = head->pos;
		head->pos += size;
		return res;
	}

	/// Вставить строку.
	const char * insert(const char * data, size_t size)
	{
		char * res = alloc(size);
		memcpy(res, data, size);
		return res;
	}
};


}
