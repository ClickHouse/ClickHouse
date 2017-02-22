#include <DB/IO/WriteBufferFromTemporaryFile.h>

#include <Poco/File.h>
#include <Poco/Path.h>

#include <DB/IO/ReadBufferFromFile.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_OPEN_FILE;
	extern const int CANNOT_SEEK_THROUGH_FILE;
}


WriteBufferFromTemporaryFile::Ptr WriteBufferFromTemporaryFile::create(const std::string & path_template_)
{
	std::string path_template = path_template_;

	if (path_template.empty() || path_template.back() != 'X')
		path_template += "XXXXXX";

	Poco::File(Poco::Path(path_template).makeParent()).createDirectories();

	int fd = mkstemp(const_cast<char *>(path_template.c_str()));
	if (fd < 0)
		throw Exception("Cannot create temporary file " + path_template, ErrorCodes::CANNOT_OPEN_FILE);

	return Ptr(new WriteBufferFromTemporaryFile(fd, path_template));
}


WriteBufferFromTemporaryFile::~WriteBufferFromTemporaryFile()
{
	/// remove temporary file if it was not passed to ReadBuffer
	if (getFD() >= 0 && !getFileName().empty())
	{
		Poco::File(getFileName()).remove();
	}
}


class ReadBufferFromTemporaryWriteBuffer : public ReadBufferFromFile
{
public:

	static ReadBufferPtr createFrom(WriteBufferFromTemporaryFile * origin)
	{
		int fd = origin->getFD();
		std::string file_name = origin->getFileName();

		off_t res = lseek(fd, 0, SEEK_SET);
		if (-1 == res)
			throwFromErrno("Cannot reread temporary file " + file_name, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

		return std::make_shared<ReadBufferFromTemporaryWriteBuffer>(fd, file_name);
	}

	ReadBufferFromTemporaryWriteBuffer(int fd, const std::string & file_name)
	: ReadBufferFromFile(fd, file_name)
	{}

	~ReadBufferFromTemporaryWriteBuffer() override
	{
		/// remove temporary file
		Poco::File(file_name).remove();
	}
};


ReadBufferPtr WriteBufferFromTemporaryFile::getReadBuffer()
{
	/// ignore buffer, write all data to file and reread it from disk
	sync();

	auto res = ReadBufferFromTemporaryWriteBuffer::createFrom(this);

	/// invalidate FD to avoid close(fd) in destructor
	setFD(-1);
	file_name = {};

	return res;
}


}
