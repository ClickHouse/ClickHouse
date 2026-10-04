#pragma once
#include "config.h"
#if USE_PCAP

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

#include <cstdio>
#include <atomic>
#include <exception>
#include <memory>

namespace Tins
{
class FileSniffer;
}

namespace DB
{

/// Reads packet capture files (pcap and pcapng) and produces one row per packet
/// with decoded L2-L4 header fields, using libtins (which reads the container
/// via libpcap). The format is a block input format: it produces whole Chunks.
class PCAPBlockInputFormat final : public IInputFormat
{
public:
    PCAPBlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_);
    ~PCAPBlockInputFormat() override;

    String getName() const override { return "PCAPBlockInputFormat"; }

    void resetParser() override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }
    void onCancel() noexcept override { is_stopped = 1; }

protected:
    Chunk read() override;

private:
    const FormatSettings format_settings;

    /// Lazily opened on the first read().
    bool initialized = false;
    std::unique_ptr<Tins::FileSniffer> sniffer;

    /// When the input is not a local file, `libpcap` reads it through a `FILE *` backed by the
    /// input buffer. The stream is owned by the sniffer once it is created.
    struct InputStreamCookie
    {
        ReadBuffer * in = nullptr;
        /// The exception thrown by the input buffer while `libpcap` was reading from it.
        std::exception_ptr exception;
    };
    InputStreamCookie stream_cookie;
    FILE * capture_file = nullptr;

    /// 1-based packet counter across the whole capture.
    size_t packet_number = 0;
    size_t approx_bytes_read_for_chunk = 0;
    std::atomic<int> is_stopped{0};

    void initializeIfNeeded();
    void closeFile();
    void rethrowInputException();

    static ssize_t readFromInput(void * cookie, char * buf, size_t size);
    static int closeInput(void * cookie);
};

class PCAPSchemaReader final : public ISchemaReader
{
public:
    explicit PCAPSchemaReader(ReadBuffer & in_);

    NamesAndTypesList readSchema() override;
};

}

#endif
