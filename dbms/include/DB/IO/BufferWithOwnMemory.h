#pragma once

#include <boost/noncopyable.hpp>

#define DBMS_DEFAULT_BUFFER_SIZE 1048576ULL


namespace DB
{


/** Замена std::vector<char> для использования в буферах.
  * Отличается тем, что не делает лишний memset. (И почти ничего не делает.)
  */
struct Memory : boost::noncopyable
{
	size_t m_capacity;
	size_t m_size;
	char * m_data;

	Memory() : m_capacity(0), m_size(0), m_data(NULL) {}
	Memory(size_t size_) : m_capacity(size_), m_size(m_capacity), m_data(size_ ? new char[m_capacity] : NULL) {}

	~Memory()
	{
		if (m_data)
		{
			delete[] m_data;
			m_data = NULL;
		}
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
			if (m_data)
				delete[] m_data;

			m_capacity = new_size;
			m_size = m_capacity;
			m_data = new char[m_capacity];
		}
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
	BufferWithOwnMemory(size_t size = DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = NULL) : Base(NULL, 0), memory(existing_memory ? 0 : size)
	{
		Base::set(existing_memory ? existing_memory : &memory[0], size);
	}
};


}
