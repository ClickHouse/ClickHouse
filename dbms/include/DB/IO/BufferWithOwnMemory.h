#pragma once

#include <boost/noncopyable.hpp>

#include <DB/Common/ProfileEvents.h>
#include <DB/Common/Allocator.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Core/Defines.h>


namespace DB
{


/** Замена std::vector<char> для использования в буферах.
  * Отличается тем, что не делает лишний memset. (И почти ничего не делает.)
  * Также можно попросить выделять выровненный кусок памяти.
  */
struct Memory : boost::noncopyable, Allocator
{
	size_t m_capacity = 0;
	size_t m_size = 0;
	char * m_data = nullptr;
	size_t alignment = 0;

    Memory() {}

	/// Если alignment != 0, то будет выделяться память, выровненная на alignment.
	Memory(size_t size_, size_t alignment_ = 0) : m_capacity(size_), m_size(m_capacity), alignment(alignment_)
	{
		alloc();
	}

	~Memory()
	{
		dealloc();
	}

	Memory(Memory && rhs)
	{
		*this = std::move(rhs);
	}

	Memory & operator=(Memory && rhs)
	{
		std::swap(m_capacity, rhs.m_capacity);
		std::swap(m_size, rhs.m_size);
		std::swap(m_data, rhs.m_data);
		std::swap(alignment, rhs.alignment);

		return *this;
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
			new_size = align(new_size);
			m_data = reinterpret_cast<char *>(Allocator::realloc(m_data, m_capacity, new_size, alignment));
			m_capacity = new_size;
			m_size = m_capacity;
		}
	}

private:
	size_t align(size_t value) const
	{
		if (!alignment)
			return value;

		return (value + alignment - 1) / alignment * alignment;
	}

	void alloc()
	{
		if (!m_capacity)
		{
			m_data = nullptr;
			return;
		}

		ProfileEvents::increment(ProfileEvents::IOBufferAllocs);
		ProfileEvents::increment(ProfileEvents::IOBufferAllocBytes, m_capacity);

		size_t new_capacity = align(m_capacity);
		m_data = reinterpret_cast<char *>(Allocator::alloc(new_capacity, alignment));
		m_capacity = new_capacity;
		m_size = m_capacity;
	}

	void dealloc()
	{
		if (!m_data)
			return;

		Allocator::free(reinterpret_cast<void *>(m_data), m_capacity);
		m_data = nullptr;	/// Чтобы избежать double free, если последующий вызов alloc кинет исключение.
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
	BufferWithOwnMemory(size_t size = DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = nullptr, size_t alignment = 0) : Base(nullptr, 0), memory(existing_memory ? 0 : size, alignment)
	{
		Base::set(existing_memory ? existing_memory : &memory[0], size);
	}
};


}
