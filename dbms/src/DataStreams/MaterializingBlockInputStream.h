#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

/** Преобразует столбцы-константы в полноценные столбцы ("материализует" их).
  */
class MaterializingBlockInputStream : public IProfilingBlockInputStream
{
public:
    MaterializingBlockInputStream(BlockInputStreamPtr input_);
    String getName() const override;
    String getID() const override;

protected:
    Block readImpl() override;
};

}
