#pragma once

#include <DataStreams/PrettyCompactBlockOutputStream.h>


namespace DB
{

/** Same as PrettyCompactBlockOutputStream, but prints all max_rows (or less,
 *     if the result contains fewer rows) by one block with one header.
  */
class PrettyCompactMonoBlockOutputStream : public PrettyCompactBlockOutputStream
{
public:
    PrettyCompactMonoBlockOutputStream(WriteBuffer & ostr_, bool no_escapes_, size_t max_rows_, const Context & context_)
        : PrettyCompactBlockOutputStream(ostr_, no_escapes_, max_rows_, context_) {}

    void write(const Block & block) override;
    void writeSuffix() override;

private:
    using Blocks_t = std::vector<Block>;

    Blocks_t blocks;
};

}
