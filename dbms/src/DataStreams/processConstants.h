#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>


namespace DB
{
/** Functions for manipulate constansts for sorting. 
  * See MergeSortingBlocksBlockInputStream and FinishSortingBlockInputStream for details.
*/

/** Remove constant columns from block.
 */
void removeConstantsFromBlock(Block & block);

void removeConstantsFromSortDescription(const Block & header, SortDescription & description);

/** Add into block, whose constant columns was removed by previous function,
  *  constant columns from header (which must have structure as before removal of constants from block).
  */
void enrichBlockWithConstants(Block & block, const Block & header);
}