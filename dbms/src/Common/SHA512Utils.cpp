#include <DB/Common/SHA512Utils.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/HexWriteBuffer.h>
#include <Poco/File.h>

#include <vector>
#include <deque>
#include <array>

namespace SHA512Utils
{

void updateHash(SHA512_CTX & ctx, const std::string & path)
{
	constexpr size_t buffer_size = 1024;

	std::deque<std::string> to_process;
	to_process.push_back(path);

	while (!to_process.empty())
	{
		std::string filename = to_process.front();
		Poco::File cur{filename};
		to_process.pop_front();

		if (cur.isFile())
		{
			DB::ReadBufferFromFile buf{filename};
			while (!buf.eof())
			{
				std::array<char, buffer_size> in;
				size_t read_count = buf.read(in.data(), in.size());
				SHA512_Update(&ctx, reinterpret_cast<const void*>(in.data()), read_count);
			}
		}
		else
		{
			std::vector<std::string> files;
			cur.list(files);
			for (const auto & file : files)
				to_process.push_back(path + "/" + file);
		}
	}
}

std::string computeHashFromString(const std::string & in)
{
	unsigned char hash[SHA512_DIGEST_LENGTH];

	SHA512_CTX ctx;
	SHA512_Init(&ctx);
	SHA512_Update(&ctx, reinterpret_cast<const void *>(in.data()), in.size());
	SHA512_Final(hash, &ctx);

	std::string out;
	{
		DB::WriteBufferFromString buf{out};
		DB::HexWriteBuffer hex_buf{buf};
		hex_buf.write(reinterpret_cast<const char *>(hash), sizeof(hash));
	}

	return out;
}

std::string computeHashFromFolder(const std::string & path)
{
	unsigned char hash[SHA512_DIGEST_LENGTH];

	SHA512_CTX ctx;
	SHA512_Init(&ctx);
	updateHash(ctx, path);
	SHA512_Final(hash, &ctx);

	std::string out;
	{
		DB::WriteBufferFromString buf{out};
		DB::HexWriteBuffer hex_buf{buf};
		hex_buf.write(reinterpret_cast<const char *>(hash), sizeof(hash));
	}

	return out;
}

}
