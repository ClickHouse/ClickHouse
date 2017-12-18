#pragma once

#include <Dictionaries/Embedded/GeodataProviders/IHierarchiesProvider.h>

#include <IO/ReadBuffer.h>


// Reads regions hierarchy in geoexport format
class RegionsHierarchyFormatReader : public IRegionsHierarchyReader
{
private:
    DB::ReadBufferPtr input;

public:
    RegionsHierarchyFormatReader(DB::ReadBufferPtr input_)
        : input(std::move(input_))
    {}

    bool readNext(RegionEntry & entry) override;
};

