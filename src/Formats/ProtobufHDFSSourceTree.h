#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_formats.h"
#endif

#if (USE_PROTOBUF && USE_HDFS)

#    include <google/protobuf/compiler/importer.h>
#    include <google/protobuf/io/zero_copy_stream.h>
#    include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#    include <IO/HDFSCommon.h>
#    include <IO/ReadBufferFromHDFS.h>

namespace DB
{
class ProtobufHDFSSourceTree : public google::protobuf::compiler::SourceTree
{
public:
    ProtobufHDFSSourceTree() { }
    ~ProtobufHDFSSourceTree() override { }

    void init(const std::string & hdfs_uri);
    google::protobuf::io::ZeroCopyInputStream * Open(const std::string & filename) override;
    std::string GetLastErrorMessage() override;

private:
    std::string last_error_message_;

    HDFSBuilderPtr builder;
    HDFSFSPtr fs;
    std::string path;
    std::string uri;
};

class ProtobufHDFSInputStream : public google::protobuf::io::ZeroCopyInputStream
{
public:
    explicit ProtobufHDFSInputStream(hdfsFS fs, hdfsFile file_descriptor, const std::string & uri);

    bool Next(const void ** data, int * size) override;
    void BackUp(int count) override;
    bool Skip(int count) override;
    int64_t ByteCount() const override;

private:
    class CopyingHDFSFileInputStream : public google::protobuf::io::CopyingInputStream
    {
    public:
        CopyingHDFSFileInputStream(hdfsFS fs, hdfsFile file_descriptor, const std::string & uri);
        ~CopyingHDFSFileInputStream() override;
        int Read(void * buffer, int size) override;
        int Skip(int count) override;

    private:
        hdfsFS fs_;
        hdfsFile file_;
        std::string uri_;
        bool previous_seek_failed_;
    };

    CopyingHDFSFileInputStream copying_input_;
    google::protobuf::io::CopyingInputStreamAdaptor impl_;
};

}
#endif
