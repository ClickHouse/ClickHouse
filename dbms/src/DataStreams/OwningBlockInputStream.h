#pragma once

#include <memory>

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

/** Provides reading from a Buffer, taking exclusive ownership over it's lifetime,
  *    simplifies usage of ReadBufferFromFile (no need to manage buffer lifetime) etc.
  */
template <typename OwnType>
class OwningBlockInputStream : public IProfilingBlockInputStream
{
public:
    OwningBlockInputStream(const BlockInputStreamPtr & stream, std::unique_ptr<OwnType> own)
        : stream{stream}, own{std::move(own)}
    {
        children.push_back(stream);
    }

    Block getHeader() const override { return children.at(0)->getHeader(); }

private:
    Block readImpl() override { return stream->read(); }

    String getName() const override { return "Owning"; }

protected:
    BlockInputStreamPtr stream;
    std::unique_ptr<OwnType> own;
};

}
