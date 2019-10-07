#pragma once

#include <Formats/PrettyBlockOutputStream.h>


namespace DB
{

/** Prints the result, aligned with spaces.
  */
class PrettySpaceBlockOutputStream : public PrettyBlockOutputStream
{
public:
    PrettySpaceBlockOutputStream(WriteBuffer & ostr_, const Block & header_, const FormatSettings & format_settings_)
        : PrettyBlockOutputStream(ostr_, header_, format_settings_) {}

    void write(const Block & block) override;
    void writeSuffix() override;
};

}
