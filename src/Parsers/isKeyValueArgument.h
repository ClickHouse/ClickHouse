#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB
{

/// True for a trailing `identifier = value` argument of a table function or a table engine, such as
/// `partition_strategy = 'hive'` of `azureBlobStorage`. Such arguments do not take a positional slot.
bool isKeyValueArgument(const IAST & argument);

}
