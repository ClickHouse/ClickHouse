#pragma once

#include <memory>

#include <DataStreams/IBlockInputStream.h>

namespace DB
{

/** Provides reading from a Buffer, taking exclusive ownership over it's lifetime,
  *    simplifies usage of ReadBufferFromFile (no need to manage buffer lifetime) etc.
  */
template <typename OwnType>
class OwningBlockInputStream : public IBlockInputStream
{
public:
    OwningBlockInputStream(const BlockInputStreamPtr & stream_, std::unique_ptr<OwnType> own_)
        : stream{stream_}, own{std::move(own_)}
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
