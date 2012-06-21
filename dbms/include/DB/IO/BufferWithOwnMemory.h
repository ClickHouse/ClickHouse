#pragma once

#include <vector>

#define DBMS_DEFAULT_BUFFER_SIZE 1048576ULL


namespace DB
{


namespace detail
{
	/** Замена std::vector<char> для использования в буферах.
	  * Отличается тем, что не делает лишний memset. (И почти ничего не делает.)
	  */
	struct Memory
	{
		size_t m_size;
		char * m_data;

		Memory() : m_size(0), m_data(NULL) {}
		Memory(size_t size_) : m_size(size_), m_data(new char[m_size]) {}

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

		/// Вызывают только чтобы увеличить размер буфера.
		void resize(size_t new_size)
		{
			if (m_data)
				delete[] m_data;
			
			m_size = new_size;
			m_data = new char[m_size];
		}
	};
}


/** Буфер, который сам владеет своим куском памяти для работы.
  * Аргумент шаблона - ReadBuffer или WriteBuffer
  */
template <typename Base>
class BufferWithOwnMemory : public Base
{
protected:
	detail::Memory memory;
public:
	BufferWithOwnMemory(size_t size = DBMS_DEFAULT_BUFFER_SIZE) : Base(NULL, 0), memory(size)
	{
		Base::set(&memory[0], size);
	}
};


}
