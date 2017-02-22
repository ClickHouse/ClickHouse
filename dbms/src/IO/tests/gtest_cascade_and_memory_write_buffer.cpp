#include <gtest/gtest.h>

#include <Poco/File.h>

#include <DB/IO/CascadeWriteBuffer.h>
#include <DB/IO/MemoryReadWriteBuffer.h>
#include <DB/IO/WriteBufferFromTemporaryFile.h>

#include <DB/IO/ConcatReadBuffer.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/copyData.h>

using namespace DB;


static std::string makeTestArray(size_t size)
{
	std::string res(size, '\0');
	for (size_t i = 0; i < res.size(); ++i)
		res[i] = i % 256;
	return res;
}

static void testCascadeBufferRedability(
	std::string data,
	CascadeWriteBuffer::WriteBufferPtrs && arg1,
	CascadeWriteBuffer::WriteBufferConstructors && arg2)
{
	CascadeWriteBuffer cascade{std::move(arg1), std::move(arg2)};

	cascade.write(&data[0], data.size());
	EXPECT_EQ(cascade.count(), data.size());

	std::vector<WriteBufferPtr> write_buffers;
	std::vector<ReadBufferPtr> read_buffers;
	std::vector<ReadBuffer *> read_buffers_raw;
	cascade.getResultBuffers(write_buffers);

	for (WriteBufferPtr & wbuf : write_buffers)
	{
		if (!wbuf)
			continue;

		auto wbuf_readable = dynamic_cast<IReadableWriteBuffer *>(wbuf.get());
		ASSERT_FALSE(!wbuf_readable);

		auto rbuf = wbuf_readable->getReadBuffer();
		ASSERT_FALSE(!rbuf);

		read_buffers.emplace_back(rbuf);
		read_buffers_raw.emplace_back(rbuf.get());
	}

	ConcatReadBuffer concat(read_buffers_raw);
	std::string decoded_data;
	{
		WriteBufferFromString decoded_data_writer(decoded_data);
		copyData(concat, decoded_data_writer);
	}

	ASSERT_EQ(data, decoded_data);
}


TEST(CascadeWriteBuffer, RereadWithTwoMemoryBuffers)
try
{
	size_t max_s = 32;
	for (size_t s = 0; s < max_s; ++s)
	{
		testCascadeBufferRedability(makeTestArray(s),
			{
				std::make_shared<MemoryWriteBuffer>(s/2, 1, 2.0),
				std::make_shared<MemoryWriteBuffer>(s - s/2, 1, 2.0)
			},
			{}
		);

		testCascadeBufferRedability(makeTestArray(s),
			{
				std::make_shared<MemoryWriteBuffer>(s, 2, 1.5),
			},
			{}
		);

		testCascadeBufferRedability(makeTestArray(s),
			{
				std::make_shared<MemoryWriteBuffer>(0, 1, 1.0),
			},
			{}
		);

		testCascadeBufferRedability(makeTestArray(s),
			{
				std::make_shared<MemoryWriteBuffer>(std::max(1ul, s/2), std::max(2ul, s/4), 0.5),
				std::make_shared<MemoryWriteBuffer>(0, 4, 1.0),
			},
			{}
		);

		testCascadeBufferRedability(makeTestArray(max_s),
			{
				std::make_shared<MemoryWriteBuffer>(s, 1, 2.0)
			},
			{
				[=] (auto prev) { return std::make_shared<MemoryWriteBuffer>(max_s - s, 1, 2.0); }
			}
		);

		testCascadeBufferRedability(makeTestArray(max_s),
			{},
			{
				[=] (auto prev) { return std::make_shared<MemoryWriteBuffer>(max_s - s, 1, 2.0); },
				[=] (auto prev) { return std::make_shared<MemoryWriteBuffer>(s, 1, 2.0); }
			}
		);
	}
}
catch (const DB::Exception & e)
{
	std::cerr << getCurrentExceptionMessage(true) << "\n";
	throw;
}
catch (...)
{
	throw;
}


TEST(TemporaryFileWriteBuffer, WriteAndReread)
try
{
	for (size_t s = 0; s < 2500000; s += 500000)
	{
		std::string tmp_template = "tmp/TemporaryFileWriteBuffer.XXXXXX";
		std::string data = makeTestArray(s);

		auto buf = WriteBufferFromTemporaryFile::create(tmp_template);
		buf->write(&data[0], data.size());

		std::string tmp_filename = buf->getFileName();
		ASSERT_EQ(tmp_template.size(), tmp_filename.size());

		auto reread_buf = buf->getReadBuffer();
		std::string decoded_data;
		{
			WriteBufferFromString wbuf_decode(decoded_data);
			copyData(*reread_buf, wbuf_decode);
		}

		ASSERT_EQ(data.size(), decoded_data.size());
		ASSERT_TRUE(data == decoded_data);

		buf.reset();
		reread_buf.reset();
		ASSERT_TRUE(!Poco::File(tmp_filename).exists());
	}
}
catch (...)
{
	std::cerr << getCurrentExceptionMessage(true) << "\n";
	throw;
}


TEST(CascadeWriteBuffer, RereadWithTemporaryFileWriteBuffer)
try
{
	const std::string tmp_template = "tmp/RereadWithTemporaryFileWriteBuffer.XXXXXX";

	for (size_t s = 0; s < 4000000; s += 1000000)
	{
		testCascadeBufferRedability(makeTestArray(s),
			{},
			{
				[=] (auto prev) { return WriteBufferFromTemporaryFile::create(tmp_template); }
			}
		);

		testCascadeBufferRedability(makeTestArray(s),
			{
				std::make_shared<MemoryWriteBuffer>(std::max(1ul, s/3ul), 2, 1.5),
			},
			{
				[=] (auto prev) { return WriteBufferFromTemporaryFile::create(tmp_template); }
			}
		);
	}
}
catch (...)
{
	std::cerr << getCurrentExceptionMessage(true) << "\n";
	throw;
}
