#include <Formats/ProtobufHDFSSourceTree.h>

#if (USE_PROTOBUF && USE_HDFS)

#include <google/protobuf/io/zero_copy_stream.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_OPEN_FILE;
}

std::string ProtobufHDFSSourceTree::GetLastErrorMessage()
{
    return last_error_message_;
}

void ProtobufHDFSSourceTree::init(const std::string & hdfs_uri)
{
    builder = createHDFSBuilder(hdfs_uri);
    fs = createHDFSFS(builder.get());
    const size_t begin_of_path = hdfs_uri.find('/', hdfs_uri.find("//") + 2);
    path = hdfs_uri.substr(begin_of_path) + "/";
    uri = hdfs_uri;
}

google::protobuf::io::ZeroCopyInputStream * ProtobufHDFSSourceTree::Open(const std::string & filename)
{
    auto fullname = path + filename;
    hdfsFile fin;
    fin = hdfsOpenFile(fs.get(), fullname.c_str(), O_RDONLY, 0, 0, 0);
    if (fin == nullptr)
        throw Exception("Unable to open HDFS file: " + path + " error: " + std::string(hdfsGetLastError()), ErrorCodes::CANNOT_OPEN_FILE);
    ProtobufHDFSInputStream * stream = new ProtobufHDFSInputStream(fs.get(), fin, uri + filename);
    return stream;
}

ProtobufHDFSInputStream::ProtobufHDFSInputStream(hdfsFS fs, hdfsFile file_descriptor, const std::string & uri)
    : copying_input_(fs, file_descriptor, uri), impl_(&copying_input_)
{
}

bool ProtobufHDFSInputStream::Next(const void ** data, int * size)
{
    return impl_.Next(data, size);
}

void ProtobufHDFSInputStream::BackUp(int count)
{
    impl_.BackUp(count);
}

bool ProtobufHDFSInputStream::Skip(int count)
{
    return impl_.Skip(count);
}

int64_t ProtobufHDFSInputStream::ByteCount() const
{
    return impl_.ByteCount();
}

ProtobufHDFSInputStream::CopyingHDFSFileInputStream::CopyingHDFSFileInputStream(
    hdfsFS fs, hdfsFile file_descriptor, const std::string & uri)
    : fs_(fs), file_(file_descriptor), uri_(uri), previous_seek_failed_(false)
{
}

ProtobufHDFSInputStream::CopyingHDFSFileInputStream::~CopyingHDFSFileInputStream()
{
    hdfsCloseFile(fs_, file_);
}

int ProtobufHDFSInputStream::CopyingHDFSFileInputStream::Read(void * buffer, int size)
{
    int bytes_read = hdfsRead(fs_, file_, buffer, size);
    if (bytes_read < 0)
        throw Exception("Fail to read HDFS file: " + uri_ + " " + std::string(hdfsGetLastError()), ErrorCodes::NETWORK_ERROR);
    return bytes_read;
}

int ProtobufHDFSInputStream::CopyingHDFSFileInputStream::Skip(int count)
{
    if (!previous_seek_failed_)
    {
        auto cur = hdfsTell(fs_, file_);
        if (hdfsSeek(fs_, file_, cur + count) != -1)
            return count;
    }
    previous_seek_failed_ = true;
    return CopyingInputStream::Skip(count);
}

}
#endif
