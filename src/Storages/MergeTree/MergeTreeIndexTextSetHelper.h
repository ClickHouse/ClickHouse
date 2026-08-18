#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/ITokenizer.h>

namespace DB
{

/// Whether an `IN` set element of `set_type` can be matched against the tokens a text or token bloom
/// filter index built from `index_type` with `tokenizer`. The element bytes are tokenized as they
/// arrive, so another representation requires tokens no granule holds and prunes a matching granule.
bool textIndexSetElementIsComparable(
    const DataTypePtr & set_type, const DataTypePtr & index_type, const ITokenizer & tokenizer, bool has_preprocessor = false);

}
