#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

/** Пустой поток блоков.
  */
class NullBlockInputStream : public IBlockInputStream
{
public:
    Block read() override { return Block(); }
    String getName() const override { return "Null"; }

    String getID() const override;
};

}
