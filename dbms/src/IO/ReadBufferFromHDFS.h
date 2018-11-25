#pragma once
#include <IO/ReadBuffer.h>
#include <Poco/URI.h>
#include <hdfs/hdfs.h>
#include <IO/BufferWithOwnMemory.h>

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
        extern const int NETWORK_ERROR;
    }
    /** Accepts path to file and opens it, or pre-opened file descriptor.
     * Closes file by himself (thus "owns" a file descriptor).
     */
    class ReadBufferFromHDFS : public BufferWithOwnMemory<ReadBuffer>
    {
        protected:
            std::string hdfs_uri;
            struct hdfsBuilder *builder;
            hdfsFS fs;
            hdfsFile fin;
        public:
            ReadBufferFromHDFS(const std::string & hdfs_name_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
                : BufferWithOwnMemory<ReadBuffer>(buf_size), hdfs_uri(hdfs_name_) , builder(hdfsNewBuilder())
            {
                Poco::URI uri(hdfs_name_);
                auto & host = uri.getHost();
                auto port = uri.getPort();
                auto & path = uri.getPath();
                if (host.empty() || port == 0 || path.empty())
                {
                    throw Exception("Illegal HDFS URI: " + hdfs_uri, ErrorCodes::BAD_ARGUMENTS);
                }
                // set read/connect timeout, default value in libhdfs3 is about 1 hour, and too large
                /// TODO Allow to tune from query Settings.
                hdfsBuilderConfSetStr(builder, "input.read.timeout", "60000"); // 1 min
                hdfsBuilderConfSetStr(builder, "input.connect.timeout", "60000"); // 1 min

                hdfsBuilderSetNameNode(builder, host.c_str());
                hdfsBuilderSetNameNodePort(builder, port);
                fs = hdfsBuilderConnect(builder);

                if (fs == nullptr)
                {
                    throw Exception("Unable to connect to HDFS: " + String(hdfsGetLastError()), ErrorCodes::NETWORK_ERROR);
                }

                fin = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
            }

            ReadBufferFromHDFS(ReadBufferFromHDFS &&) = default;

            ~ReadBufferFromHDFS() override
            {
                close();
                hdfsFreeBuilder(builder);
            }

            /// Close HDFS connection before destruction of object.
            void close()
            {
                hdfsCloseFile(fs, fin);
            }

            bool nextImpl() override
            {
                int bytes_read = hdfsRead(fs, fin, internal_buffer.begin(), internal_buffer.size());
                if (bytes_read < 0)
                {
                    throw Exception("Fail to read HDFS file: " + hdfs_uri + " " + String(hdfsGetLastError()), ErrorCodes::NETWORK_ERROR);
                }

                if (bytes_read)
                    working_buffer.resize(bytes_read);
                else
                    return false;
                return true;
            }

            const std::string & getHDFSUri() const
            {
                return hdfs_uri;
            }
    };
}
