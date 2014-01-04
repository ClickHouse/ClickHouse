#pragma once

#include <boost/noncopyable.hpp>

#include <DB/Common/ProfileEvents.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#define DBMS_DEFAULT_BUFFER_SIZE 1048576ULL


namespace DB
{


/** Замена std::vector<char> для использования в буферах.
  * Отличается тем, что не делает лишний memset. (И почти ничего не делает.)
  * Также можно попросить выделять выровненный кусок памяти.
  */
struct Memory : boost::noncopyable
{
	size_t m_capacity;
	size_t m_size;
	char * m_data;
	size_t alignment;

	Memory() : m_capacity(0), m_size(0), m_data(NULL), alignment(0) {}

	/// Если alignment != 0, то будет выделяться память, выровненная на alignment.
	Memory(size_t size_, size_t alignment_ = 0) : m_capacity(size_), m_size(m_capacity), alignment(alignment_)
	{
		alloc();
	}

	~Memory()
	{
		dealloc();
	}

	size_t size() const { return m_size; }
	const char & operator[](size_t i) const { return m_data[i]; }
	char & operator[](size_t i) { return m_data[i]; }

	void resize(size_t new_size)
	{
		if (new_size < m_capacity)
		{
			m_size = new_size;
			return;
		}
		else
		{
			dealloc();

			m_capacity = new_size;
			m_size = m_capacity;

			alloc();
		}
	}

private:
	void alloc()
	{
		if (!m_capacity)
		{
			m_data = NULL;
			return;
		}

		ProfileEvents::increment(ProfileEvents::IOBufferAllocs);
		ProfileEvents::increment(ProfileEvents::IOBufferAllocBytes, m_capacity);

		if (!alignment)
		{
			m_data = reinterpret_cast<char *>(malloc(m_capacity));

			if (!m_data)
				throw Exception("Cannot allocate memory (malloc)", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

			return;
		}

		int res = posix_memalign(reinterpret_cast<void **>(&m_data), alignment, (m_capacity + alignment - 1) / alignment * alignment);

		if (0 != res)
			DB::throwFromErrno("Cannot allocate memory (posix_memalign)", ErrorCodes::CANNOT_ALLOCATE_MEMORY, res);
	}

	void dealloc()
	{
		if (m_data)
			free(reinterpret_cast<void *>(m_data));
	}
};


/** Буфер, который может сам владеть своим куском памяти для работы.
  * Аргумент шаблона - ReadBuffer или WriteBuffer
  */
template <typename Base>
class BufferWithOwnMemory : public Base
{
protected:
	Memory memory;
public:
	/// Если передать не-NULL existing_memory, то буфер не будет создавать свой кусок памяти, а будет использовать существующий (и не будет им владеть).
	BufferWithOwnMemory(size_t size = DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = NULL, size_t alignment = 0) : Base(NULL, 0), memory(existing_memory ? 0 : size, alignment)
	{
		Base::set(existing_memory ? existing_memory : &memory[0], size);
	}
};


}
