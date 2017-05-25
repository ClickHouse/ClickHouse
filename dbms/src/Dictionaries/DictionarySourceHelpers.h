#pragma once

#include <vector>
#include <common/Types.h>
#include <Columns/IColumn.h>


namespace DB
{

class IBlockOutputStream;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;

struct DictionaryStructure;

/// Write keys to block output stream.

/// For simple key
void formatIDs(BlockOutputStreamPtr & out, const std::vector<UInt64> & ids);

/// For composite key
void formatKeys(const DictionaryStructure & dict_struct, BlockOutputStreamPtr & out, const ConstColumnPlainPtrs & key_columns);

}
