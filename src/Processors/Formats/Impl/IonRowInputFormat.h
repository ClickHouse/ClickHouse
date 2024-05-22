#pragma once

#include "config.h"

#if USE_ION

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowInputFormat.h>

#include <Formats/IonReader.h>

namespace DB
{

class ReadBuffer;

class IonRowInputFormat final : public IRowInputFormat
{
public:
    IonRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_);

    String getName() const override { return "IonRowInputFormat"; }

private:
    void readPrefix() override;
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;

    void deserializeField(IColumn & column, DataTypePtr type);

    std::unique_ptr<IonReader> reader;
    DataTypes data_types;
};

}

#endif
