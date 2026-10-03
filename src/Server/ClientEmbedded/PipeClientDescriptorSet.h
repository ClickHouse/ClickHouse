#pragma once

#include <Server/ClientEmbedded/IClientDescriptorSet.h>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <Poco/Pipe.h>
#include <Common/Exception.h>

namespace DB
{

#if defined(OS_WINDOWS)
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
#endif


class PipeClientDescriptorSet : public IClientDescriptorSet
{
public:
    PipeClientDescriptorSet()
        : fd_source(pipe_in.readHandle(), boost::iostreams::never_close_handle)
        , fd_sink(pipe_out.writeHandle(), boost::iostreams::never_close_handle)
        , fd_sink_err(pipe_err.writeHandle(), boost::iostreams::never_close_handle)
        , input_stream(fd_source)
        , output_stream(fd_sink)
        , output_stream_err(fd_sink_err)
    {
        output_stream << std::unitbuf;
        output_stream_err << std::unitbuf;
    }

    /// `Poco::Pipe::Handle` is a file descriptor on POSIX but a `HANDLE` on Windows, and a
    /// `HANDLE` is not a descriptor - it would have to be adopted into one with
    /// `_open_osfhandle`, which transfers ownership away from the `Poco::Pipe` that is going to
    /// close it. Since the embedded client this feeds is server-side, that is left undone.
#if defined(OS_WINDOWS)
    DescriptorSet getDescriptorsForClient() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The embedded client is not implemented on Windows");
    }

    DescriptorSet getDescriptorsForServer() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The embedded client is not implemented on Windows");
    }
#else
    DescriptorSet getDescriptorsForClient() override
    {
        return DescriptorSet{.in = pipe_in.readHandle(), .out = pipe_out.writeHandle(), .err = pipe_err.writeHandle()};
    }

    DescriptorSet getDescriptorsForServer() override
    {
        return DescriptorSet{.in = pipe_in.writeHandle(), .out = pipe_out.readHandle(), .err = pipe_err.readHandle()};
    }
#endif

    StreamSet getStreamsForClient() override { return StreamSet{.in = input_stream, .out = output_stream, .err = output_stream_err}; }

    void closeServerDescriptors() override
    {
        pipe_in.close(Poco::Pipe::CLOSE_WRITE);
        pipe_out.close(Poco::Pipe::CLOSE_READ);
        pipe_err.close(Poco::Pipe::CLOSE_READ);
    }

    void closeStdIn() override
    {
        pipe_in.close(Poco::Pipe::CLOSE_WRITE);
    }

    bool isPty() const override { return false; }

    ~PipeClientDescriptorSet() override = default;

private:
    Poco::Pipe pipe_in;
    Poco::Pipe pipe_out;
    Poco::Pipe pipe_err;

    // Provide streams on top of file descriptors
    boost::iostreams::file_descriptor_source fd_source;
    boost::iostreams::file_descriptor_sink fd_sink;
    boost::iostreams::file_descriptor_sink fd_sink_err;
    boost::iostreams::stream<boost::iostreams::file_descriptor_source> input_stream;
    boost::iostreams::stream<boost::iostreams::file_descriptor_sink> output_stream;
    boost::iostreams::stream<boost::iostreams::file_descriptor_sink> output_stream_err;
};

}
