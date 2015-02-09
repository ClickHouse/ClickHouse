/** Воспроизводит баг в gcc 4.8.2
  * Баг: исключение не ловится.
  *
  * /usr/bin/c++ -std=c++11 -Wall -O3 ./io_and_exceptions.cpp && ./a.out
  *
  * Выводит:
  * terminate called after throwing an instance of 'int'
  * Aborted
  *
  * А должно ничего не выводить.
  *
  * В gcc 4.9 и clang 3.6 всё Ок.
  */

typedef unsigned long size_t;

class BufferBase
{
public:
	typedef char * Position;

	struct Buffer
	{
		Buffer(Position begin_pos_, Position end_pos_) : begin_pos(begin_pos_), end_pos(end_pos_) {}

		inline Position begin() const { return begin_pos; }
		inline Position end() const { return end_pos; }
		inline size_t size() const { return end_pos - begin_pos; }
		inline void resize(size_t size) { end_pos = begin_pos + size; }

	private:
		Position begin_pos;
		Position end_pos;
	};

	BufferBase(Position ptr, size_t size, size_t offset)
		: internal_buffer(ptr, ptr + size), working_buffer(ptr, ptr + size), pos(ptr + offset),	bytes(0) {}

	void set(Position ptr, size_t size, size_t offset)
	{
		internal_buffer = Buffer(ptr, ptr + size);
		working_buffer = Buffer(ptr, ptr + size);
		pos = ptr + offset;
	}

	inline Buffer & buffer() { return working_buffer; }
	inline Position & position() { return pos; };
	inline size_t offset() const { return pos - working_buffer.begin(); }

	size_t count() const
	{
		return bytes + offset();
	}

protected:
	Buffer internal_buffer;
	Buffer working_buffer;

	Position pos;
	size_t bytes;
};


class ReadBuffer : public BufferBase
{
public:
	ReadBuffer(Position ptr, size_t size) : BufferBase(ptr, size, 0) { working_buffer.resize(0); }
	ReadBuffer(Position ptr, size_t size, size_t offset) : BufferBase(ptr, size, offset) {}

	inline bool next()
	{
		bytes += offset();
		bool res = nextImpl();
		if (!res)
			working_buffer.resize(0);

		pos = working_buffer.begin();
		return res;
	}

	virtual ~ReadBuffer() {}

	inline bool eof()
	{
		return pos == working_buffer.end() && !next();
	}

private:
	virtual bool nextImpl() { return false; };
};


class CompressedReadBuffer : public ReadBuffer
{
private:
	bool nextImpl()
	{
		throw 1;
		return true;
	}

public:
	CompressedReadBuffer() : ReadBuffer(nullptr, 0)
	{
	}
};


void readIntText(unsigned & x, ReadBuffer & buf)
{
	x = 0;
	while (!buf.eof())
	{
		switch (*buf.position())
		{
			case '+':
				break;
			case '9':
				x *= 10;
				break;
			default:
				return;
		}
	}
}


unsigned parse(const char * data)
{
	unsigned res;
	ReadBuffer buf(const_cast<char *>(data), 10, 0);
	readIntText(res, buf);
	return res;
}



int main()
{
	CompressedReadBuffer in;

	try
	{
		while (!in.eof())
			;
	}
	catch (...)
	{
	}

	return 0;
}


void f()
{
	parse("123");
}
