#pragma once

#include <Dictionaries/Embedded/GeodataProviders/INamesProvider.h>

#include <IO/ReadBuffer.h>


// Reads regions names list in geoexport format
class LanguageRegionsNamesFormatReader : public ILanguageRegionsNamesReader
{
private:
    DB::ReadBufferPtr input;

public:
    LanguageRegionsNamesFormatReader(DB::ReadBufferPtr input_)
        : input(std::move(input_))
    {}

    bool readNext(RegionNameEntry & entry) override;
};
