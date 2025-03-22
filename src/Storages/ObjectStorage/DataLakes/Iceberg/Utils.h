#pragma once

#include "config.h"

#include <string>
#include <string_view>

#if USE_AVRO

namespace Iceberg
{

std::string getProperFilePathFromMetadataInfo(std::string_view data_path, std::string_view common_path, std::string_view table_location);

}

#endif
