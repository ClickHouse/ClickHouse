#pragma once

#include <DataStreams/PrettyBlockOutputStream.h>


namespace DB
{

/** Prints the result, aligned with spaces.
  */
class PrettySpaceBlockOutputStream : public PrettyBlockOutputStream
{
public:
    PrettySpaceBlockOutputStream(WriteBuffer & ostr_, const Block & header_, bool no_escapes_, size_t max_rows_, const Context & context_)
        : PrettyBlockOutputStream(ostr_, header_, no_escapes_, max_rows_, context_) {}

    void write(const Block & block) override;
    void writeSuffix() override;
};

}
