#pragma once

#include <Processors/ISource.h>

namespace DB
{

struct TemporaryFileStream;

class TemporaryFileLazySource : public ISource
{
public:
    TemporaryFileLazySource(const std::string & path_, const Block & header_);
    ~TemporaryFileLazySource() override;
    String getName() const override { return "TemporaryFileLazySource"; }

protected:
    Chunk generate() override;

private:
    const std::string path;
    Block header;
    bool done;

    std::unique_ptr<TemporaryFileStream> stream;
};

}
