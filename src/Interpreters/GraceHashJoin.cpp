#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>

namespace DB
{

GraceHashJoin::GraceHashJoin(std::shared_ptr<TableJoin> table_join, const Block & right_sample_block, bool any_take_last_row)
    : first_bucket{std::make_shared<HashJoin>(table_join, right_sample_block, any_take_last_row)}
{
}

bool GraceHashJoin::addJoinedBlock(const Block & block, bool check_limits) {
}

void GraceHashJoin::checkTypesOfKeys(const Block & block) const {
    return first_bucket->checkTypesOfKeys(block);
}

void GraceHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) {
}

}
