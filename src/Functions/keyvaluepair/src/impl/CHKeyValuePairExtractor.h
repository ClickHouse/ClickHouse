#pragma once

#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include "state/InlineEscapingKeyStateHandler.h"
#include "state/InlineEscapingValueStateHandler.h"


namespace DB
{

class CHKeyValuePairExtractor
{
public:
    using Response = ColumnMap::Ptr;

    CHKeyValuePairExtractor(InlineEscapingKeyStateHandler key_state_handler_, InlineEscapingValueStateHandler value_state_handler_);

    uint64_t extract(const std::string_view & data, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values);

private:

    NextState processState(std::string_view file, std::size_t pos, State state, std::string & key,
                           std::string & value, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values, uint64_t & row_offset);

    static NextState flushPair(const std::string_view & file, std::size_t pos, std::string & key,
                        std::string & value, ColumnString::MutablePtr & keys, ColumnString::MutablePtr & values, uint64_t & row_offset);

    InlineEscapingKeyStateHandler key_state_handler;
    InlineEscapingValueStateHandler value_state_handler;
};

}
