#pragma once

#include <base/types.h>

#include <Common/NamePrompter.h>

namespace DB
{

class HTTPPathHints : public IHints<>
{
public:
    VectorWithMemoryTracking<String> getAllRegisteredNames() const override;
    void add(const String & http_path);

private:
    VectorWithMemoryTracking<String> http_paths;
};

using HTTPPathHintsPtr = std::shared_ptr<HTTPPathHints>;

}
