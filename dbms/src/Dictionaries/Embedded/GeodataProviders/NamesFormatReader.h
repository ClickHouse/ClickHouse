#pragma once

#include <IO/ReadBuffer.h>
#include "INamesProvider.h"


// Reads regions names list in geoexport format
class LanguageRegionsNamesFormatReader : public ILanguageRegionsNamesReader
{
private:
    DB::ReadBufferPtr input;

public:
    LanguageRegionsNamesFormatReader(DB::ReadBufferPtr input_) : input(std::move(input_)) {}

    bool readNext(RegionNameEntry & entry) override;
};
