#pragma once

#include "config.h"

#if USE_LANCE

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>

namespace DB
{

class CHColumnToArrowColumn;

class LanceSink final : public SinkToStorage
{
public:
    LanceSink(SharedHeader sample_block_, Lance::DatasetOptions options);
    ~LanceSink() override;

    String getName() const override { return "LanceSink"; }

    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    const SharedHeader sample_block;
    Lance::Writer writer;
    std::unique_ptr<CHColumnToArrowColumn> converter;
};

}

#endif
