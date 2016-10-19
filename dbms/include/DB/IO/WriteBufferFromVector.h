#pragma once

#include <vector>

#include <DB/IO/WriteBuffer.h>


#define WRITE_BUFFER_FROM_VECTOR_INITIAL_SIZE_IF_EMPTY 32


namespace DB
{

/** Инициализируется вектором. Пишет данные в него. Когда вектор закончится - увеличивает его размер в два раза.
  * CharType - char или unsigned char.
  */
template <typename VectorType = std::vector<char> >
class WriteBufferFromVector : public WriteBuffer
{
private:
	VectorType & vector;

	void nextImpl() override
	{
		size_t old_size = vector.size();
		vector.resize(old_size * 2);
		internal_buffer = Buffer(reinterpret_cast<Position>(&vector[old_size]), reinterpret_cast<Position>(&*vector.end()));
		working_buffer = internal_buffer;
	}

public:
	WriteBufferFromVector(VectorType & vector_)
		: WriteBuffer(reinterpret_cast<Position>(&vector_[0]), vector_.size()), vector(vector_)
	{
		if (vector.empty())
		{
			vector.resize(WRITE_BUFFER_FROM_VECTOR_INITIAL_SIZE_IF_EMPTY);
			set(reinterpret_cast<Position>(&vector[0]), vector.size());
		}
	}
};

}
