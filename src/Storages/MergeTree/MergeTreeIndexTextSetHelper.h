#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/ITokenizer.h>

namespace DB
{

/// Whether an `IN` set element of `set_type` can be matched against the tokens a text or token bloom
/// filter index built from `index_type` with `tokenizer`. The element bytes are tokenized as they
/// arrive, so another representation requires tokens no granule holds and prunes a matching granule.
/// `preprocessor_is_case_folding` marks a preprocessor that maps bytes one to one and reads no type,
/// so it yields the same tokens on a set element and on the index column whatever the carrier is.
bool textIndexSetElementIsComparable(
    const DataTypePtr & set_type,
    const DataTypePtr & index_type,
    const ITokenizer & tokenizer,
    bool has_preprocessor = false,
    bool preprocessor_is_case_folding = false);

}
