#pragma once

#include <base/types.h>

#include <Common/NamePrompter.h>

namespace DB
{

class HTTPPathHints : public IHints<>
{
public:
    std::vector<String> getAllRegisteredNames() const override;
    void add(const String & http_path);

private:
    std::vector<String> http_paths;
};

using HTTPPathHintsPtr = std::shared_ptr<HTTPPathHints>;

}
