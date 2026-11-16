#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <Processors/Formats/Impl/PuffinCommon.h>
#include <DataTypes/IDataType.h>

#include <theta_sketch.hpp>
#include <theta_union.hpp>

namespace DB
{

class IColumn;
class PuffinWriter;

class PuffinThetaSketch
{
public:
    PuffinThetaSketch(const String & column_name, const DataTypePtr & type);

    void add(const IColumn & column);

    PuffinBlob write(PuffinWriter & writer, Int32 field_id, Int64 snapshot_id, Int64 sequence_number) const;

    void reset();

private:
    void update(const void * data, size_t size);

    DataTypePtr type;
    datasketches::update_theta_sketch sketch;
};

}

#endif
