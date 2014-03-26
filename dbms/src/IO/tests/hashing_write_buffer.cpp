#include <DB/IO/HashingWriteBuffer.h>
#include <DB/IO/WriteBufferFromFile.h>

#define FAIL(msg) { std::cout << msg; exit(1); }


uint128 referenceHash(char * data, size_t len)
{
	const size_t block_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE;
	uint128 state(0, 0);
	size_t pos;

	for (pos = 0; pos + block_size <= len; pos += block_size)
	{
		state = CityHash128WithSeed(data + pos, block_size, state);
	}

	if (pos < len)
		state = CityHash128WithSeed(data + pos, len - pos, state);

	return state;
}


void test(size_t data_size)
{
	std::vector<char> vec(data_size);
	char * data = &vec[0];

	for (size_t i = 0; i < data_size; ++i)
		data[i] = rand() & 255;

	uint128 reference = referenceHash(data, data_size);

	DB::WriteBufferFromFile sink("/dev/null", 1 << 16);

	{
		DB::HashingWriteBuffer buf(sink);

		for (size_t pos = 0; pos < data_size;)
		{
			size_t len = std::min(static_cast<size_t>(rand() % 10000 + 1), data_size - pos);
			buf.write(data + pos, len);
			buf.next();
			pos += len;
		}

		if (buf.getHash() != reference)
			FAIL("failed on data size " << data_size << " writing random chunks of up to 10000 bytes");
	}

	{
		DB::HashingWriteBuffer buf(sink);

		for (size_t pos = 0; pos < data_size;)
		{
			size_t len = std::min(static_cast<size_t>(rand() % 5 + 1), data_size - pos);
			buf.write(data + pos, len);
			buf.next();
			pos += len;
		}

		if (buf.getHash() != reference)
			FAIL("failed on data size " << data_size << " writing random chunks of up to 5 bytes");
	}

	{
		DB::HashingWriteBuffer buf(sink);

		for (size_t pos = 0; pos < data_size;)
		{
			size_t len = std::min(static_cast<size_t>(2048 + rand() % 3 - 1), data_size - pos);
			buf.write(data + pos, len);
			buf.next();
			pos += len;
		}

		if (buf.getHash() != reference)
			FAIL("failed on data size " << data_size << " writing random chunks of 2048 +-1 bytes");
	}

	{
		DB::HashingWriteBuffer buf(sink);

		buf.write(data, data_size);

		if (buf.getHash() != reference)
			FAIL("failed on data size " << data_size << " writing all at once");
	}
}

int main()
{
	test(5);
	test(100);
	test(2048);
	test(2049);
	test(100000);
	test(1 << 17);

	return 0;
}
