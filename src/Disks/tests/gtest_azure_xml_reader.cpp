#include <string>
#include <vector>
#include <Common/logger_useful.h>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <azure/storage/blobs.hpp>
#include <azure/storage/common/internal/xml_wrapper.hpp>

#include <gtest/gtest.h>


TEST(AzureXMLWrapper, TestLeak)
{
    std::string str = "<hello>world</hello>";

    Azure::Storage::_internal::XmlReader reader(str.c_str(), str.length());
    Azure::Storage::_internal::XmlReader reader2(std::move(reader));
    Azure::Storage::_internal::XmlReader reader3 = std::move(reader2);
    reader3.Read();
}

#endif
