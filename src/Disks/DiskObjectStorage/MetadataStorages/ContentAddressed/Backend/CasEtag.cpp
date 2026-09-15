#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>

#include <boost/algorithm/string/trim.hpp>

#include <algorithm>

namespace DB::Cas
{

bool isIncarnationValue(Dialect dialect, const String & value)
{
    switch (dialect)
    {
        case Dialect::Generation:
        {
            if (value.empty() || value == "0")
                return false;
            if (value.size() > 1 && value.front() == '0')
                return false;
            return std::all_of(value.begin(), value.end(), [](char c) { return c >= '0' && c <= '9'; });
        }
        case Dialect::ETag:
        {
            String trimmed = value;
            boost::algorithm::trim(trimmed);
            return !trimmed.empty() && trimmed != "*" && trimmed.find(',') == String::npos;
        }
        case Dialect::Emulated:
            return !value.empty();
    }
    return false;
}

}
