#pragma once

#include <mutex>
#include <unordered_set>
#include <vector>
#include <base/types.h>


namespace DB
{

class KnownObjectNames
{
public:
    bool exists(const String & name) const;
    void add(const String & name, bool case_insensitive = false);

    std::vector<String> getAllRegisteredNames() const;

private:
    mutable std::mutex mutex;
    std::unordered_set<String> names;
    std::unordered_set<String> case_insensitive_names;
};


class KnownTableFunctionNames : public KnownObjectNames
{
public:
    static KnownTableFunctionNames & instance();
};


class KnownFormatNames : public KnownObjectNames
{
public:
    static KnownFormatNames & instance();
};

}
