#pragma once

#include <Common/StringUtils/StringUtils.h>
#include <Core/Block.h>
#include <base/bit_cast.h>
#include <base/range.h>

#include "LibraryInterface.h"


namespace DB
{

class CStringsHolder
{

public:
    using Container = std::vector<std::string>;

    explicit CStringsHolder(const Container & strings_pass)
    {
        strings_holder = strings_pass;
        strings.size = strings_holder.size();

        ptr_holder = std::make_unique<ClickHouseLibrary::CString[]>(strings.size);
        strings.data = ptr_holder.get();

        size_t i = 0;
        for (auto & str : strings_holder)
        {
            strings.data[i] = str.c_str();
            ++i;
        }
    }

    ClickHouseLibrary::CStrings strings; // will pass pointer to lib

private:
    std::unique_ptr<ClickHouseLibrary::CString[]> ptr_holder = nullptr;
    Container strings_holder;
};


}
