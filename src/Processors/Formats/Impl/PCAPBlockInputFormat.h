#pragma once
#include "config.h"
#if USE_PCAP

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

#include <cstdio>
#include <atomic>
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

    /// When the input is not a local file, we copy it into a temporary file;
    /// the FILE * must outlive the sniffer.
    FILE * capture_file = nullptr;

    /// 1-based packet counter across the whole capture.
    size_t packet_number = 0;
    size_t approx_bytes_read_for_chunk = 0;
    std::atomic<int> is_stopped{0};

    void initializeIfNeeded();
    void closeFile();
};

class PCAPSchemaReader final : public ISchemaReader
{
public:
    explicit PCAPSchemaReader(ReadBuffer & in_);

    NamesAndTypesList readSchema() override;
};

}

#endif
