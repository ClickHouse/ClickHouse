#pragma once

#include <IO/ReadBuffer.h>
#include "INamesProvider.h"

namespace DB
{

// Reads regions names list in geoexport format
class LanguageRegionsNamesFormatReader : public ILanguageRegionsNamesReader
{
private:
    ReadBufferPtr input;

public:
    explicit LanguageRegionsNamesFormatReader(ReadBufferPtr input_) : input(std::move(input_)) {}

    bool readNext(RegionNameEntry & entry) override;
};

}
