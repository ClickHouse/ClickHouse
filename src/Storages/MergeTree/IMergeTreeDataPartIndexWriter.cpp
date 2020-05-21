#include "IMergeTreeDataPartIndexWriter.h"

namespace DB
{

IMergeTreeDataPartIndexWriter::IMergeTreeDataPartIndexWriter(IMergeTreeDataPartWriter & part_writer_)
    : part_writer(part_writer_)
{
}

}
