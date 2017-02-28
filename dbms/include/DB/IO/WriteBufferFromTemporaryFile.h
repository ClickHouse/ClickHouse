#include <DB/IO/WriteBuffer.h>
#include <DB/IO/IReadableWriteBuffer.h>
#include <DB/IO/WriteBufferFromFile.h>

namespace DB
{

/// Rereadable WriteBuffer, could be used as disk buffer
/// Creates unique temporary in directory (and directory itself)
class WriteBufferFromTemporaryFile : public WriteBufferFromFile, public IReadableWriteBuffer
{
public:
	using Ptr = std::shared_ptr<WriteBufferFromTemporaryFile>;

	/// path_template examle "/opt/clickhouse/tmp/data.XXXXXX"
	static Ptr create(const std::string & path_template_);

	~WriteBufferFromTemporaryFile() override;

protected:

	std::shared_ptr<ReadBuffer> getReadBufferImpl() override;

	WriteBufferFromTemporaryFile(int fd, const std::string & tmp_path)
	: WriteBufferFromFile(fd, tmp_path)
	{}
};

}
